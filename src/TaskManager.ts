import { SQS, AWSError } from 'aws-sdk';
import {
  SendMessageRequest,
  DeleteMessageBatchRequest,
  DeleteMessageBatchRequestEntryList,
  SendMessageBatchRequestEntry,
  BatchResultErrorEntry,
  ChangeMessageVisibilityBatchRequest,
  ChangeMessageVisibilityBatchRequestEntryList
} from 'aws-sdk/clients/sqs';
import { serialize, deserialize } from 'class-transformer';
import { RedisClient, RedisConfig } from '@diff./redis-client';
import { DuplicatedCheckInError } from './errors/DuplicatedCheckInError';
import { Task } from './Task';
import { FIFOTask } from './FIFOTask';
import { TaskCarrier } from './TaskCarrier';
import { PromiseResult } from 'aws-sdk/lib/request';
import { PromiseUtils, ArrayUtils } from './utils';
import { ConfigManager } from '@diff./config-manager';

const RECEIVE_MAX_NUMBER_OF_TASK = 10;
const RECEIVE_WAIT_SECOND = 20;
const CHECK_OUT_HOLDING_TTL = 180; // 3분
const CHECK_IN_DEFAULT_TTL = 600; // 10분

// addTasks 등의 벌크 리퀘스트 실행시, 한번에 요청에 포함될 수있는 항목이 10개로 제한됨(SQS제한사항)
// 이를 극복하기 위해 동시에 지정된 수 만금의 요청을 동시 발송하여 처리 속도 개선
const BULK_CONCURRENCY_COUNT = 50;

// 체크인 정보를 저장할 레디스 호스트
const REDIS_HOST = 'tm';

export class TaskManager {
  // 리전별 SQS 인스턴스
  private readonly sqsClientMap: Map<string, SQS> = new Map();

  // 테스크 URI에서 추출한 region 정보를 캐싱, 메소드 반복 호출이 많은 관계로 성능 개선 목적
  private readonly uriRegionCache: Map<string, string> = new Map();

  // ConfigManager
  private readonly config: ConfigManager<RedisConfig>;

  public constructor(config: ConfigManager<RedisConfig>) {
    this.config = config;
  }

  private isFIFOTask(task: Task | FIFOTask): task is FIFOTask {
    return (task as FIFOTask).taskGroupId !== undefined;
  }

  public async addTask(task: Task): Promise<string> {
    const params: SendMessageRequest = {
      QueueUrl: task.taskUri(),
      MessageBody: serialize(task, { strategy: 'excludeAll' })
    };

    // FIFO Taks 인 경우 그룹 ID 지정
    // https://docs.aws.amazon.com/ko_kr/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
    if (this.isFIFOTask(task)) {
      params.MessageGroupId = task.taskGroupId();
      if (task.taskDeduplicationId()) params.MessageDeduplicationId = task.taskDeduplicationId();
      if (task.taskDelay()) {
        console.warn('FIFOTask 는 개별 큐에대한 지연발송(taskDelay)설정을 지원하지 않습니다. SQS설정에서 일괄 지정하세요.');
      }
    } else {
      if (task.taskDelay()) params.DelaySeconds = task.taskDelay();
    }

    const res = await this.sqsClient(task.taskUri())
      .sendMessage(params)
      .promise();

    if (!res.MessageId) throw new Error('cannot add task : messageId is Empty');
    return res.MessageId;
  }

  public async addTasks(
    tasks: Task[],
    concurrency = BULK_CONCURRENCY_COUNT
  ): Promise<{ sendedIds: string[]; failedResults: BatchResultErrorEntry[] }> {
    const groupByUri: { [key: string]: Task[] } = {};
    for (const task of tasks) {
      if (!groupByUri[task.taskUri()]) groupByUri[task.taskUri()] = [];
      groupByUri[task.taskUri()].push(task);
    }

    const promises: Promise<PromiseResult<SQS.Types.SendMessageBatchResult, AWSError>>[] = [];
    for (const taskUri of Object.keys(groupByUri)) {
      const taskGroup = groupByUri[taskUri];
      const chunks = ArrayUtils.chunk(taskGroup, 10);

      for (const chunk of chunks) {
        let sendId = 0;
        const entries = chunk.map(task => {
          const payload: SendMessageBatchRequestEntry = {
            Id: sendId.toString(),
            MessageBody: serialize(task, { strategy: 'excludeAll' })
          };

          if (this.isFIFOTask(task)) {
            payload.MessageGroupId = task.taskGroupId();
            if (task.taskDeduplicationId()) payload.MessageDeduplicationId = task.taskDeduplicationId();
            if (task.taskDelay()) {
              console.warn('FIFOTask 는 개별 큐에대한 지연발송(taskDelay)설정을 지원하지 않습니다. SQS설정에서 일괄 지정하세요.');
            }
          } else {
            if (task.taskDelay()) payload.DelaySeconds = task.taskDelay();
          }

          sendId++;
          return payload;
        });

        const params: SQS.Types.SendMessageBatchRequest = {
          QueueUrl: taskUri,
          Entries: entries
        };

        promises.push(
          this.sqsClient(taskUri)
            .sendMessageBatch(params)
            .promise()
        );
      }
    }

    // 실행
    const promisesChunk = ArrayUtils.chunk(promises, concurrency);
    const sendedIds: string[] = [];
    const failedResults: BatchResultErrorEntry[] = [];
    for (const promis of promisesChunk) {
      const bulkResults = await PromiseUtils.all(promis);

      // API 요청에 대한 에러가 발생한 경우
      if (bulkResults.failed.length) {
        throw bulkResults.failed[0];
      }

      for (const result of bulkResults.successful) {
        sendedIds.push(...result.Successful.map(v => v.MessageId));
        failedResults.push(...result.Failed);
      }
    }

    return {
      sendedIds,
      failedResults
    };
  }

  public async receiveTasks<T extends Task>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    cls: { new (...args: any[]): T },
    maxTaskCount = RECEIVE_MAX_NUMBER_OF_TASK,
    receiveWaitSecond = RECEIVE_WAIT_SECOND
  ): Promise<TaskCarrier<T>[]> {
    const taskUri = new cls().taskUri();
    const res = await this.sqsClient(taskUri)
      .receiveMessage({
        QueueUrl: taskUri,
        MaxNumberOfMessages: maxTaskCount,
        WaitTimeSeconds: receiveWaitSecond
      })
      .promise();

    const tasks: TaskCarrier<T>[] = [];
    if (res.Messages) {
      for (const msg of res.Messages) {
        if (!msg.MessageId || !msg.ReceiptHandle || !msg.Body) continue;
        const carrier = new TaskCarrier<T>();
        carrier.messageId = msg.MessageId;
        carrier.receiptHandle = msg.ReceiptHandle;
        carrier.task = deserialize(cls, msg.Body);

        tasks.push(carrier);
      }
    }
    return tasks;
  }

  public async deleteTask<T extends Task>(taskCarrier: TaskCarrier<T>): Promise<void> {
    await this.sqsClient(taskCarrier.task.taskUri())
      .deleteMessage({
        QueueUrl: taskCarrier.task.taskUri(),
        ReceiptHandle: taskCarrier.receiptHandle
      })
      .promise();
  }

  public async deleteTasks<T extends Task>(
    taskCarriers: TaskCarrier<T>[],
    concurrency = BULK_CONCURRENCY_COUNT
  ): Promise<{ deletedIds: string[]; failedResults: BatchResultErrorEntry[] }> {
    const groupByUri: { [key: string]: TaskCarrier<T>[] } = {};
    for (const carrier of taskCarriers) {
      if (!groupByUri[carrier.task.taskUri()]) groupByUri[carrier.task.taskUri()] = [];
      groupByUri[carrier.task.taskUri()].push(carrier);
    }

    const promises: Promise<PromiseResult<SQS.Types.DeleteMessageBatchResult, AWSError>>[] = [];
    for (const taskUri of Object.keys(groupByUri)) {
      const carrierGroup = groupByUri[taskUri];
      const chunks = ArrayUtils.chunk(carrierGroup, 10);

      for (const chunk of chunks) {
        const entries: DeleteMessageBatchRequestEntryList = [];
        for (const carrier of chunk) {
          entries.push({
            Id: carrier.messageId,
            ReceiptHandle: carrier.receiptHandle
          });
        }

        const params: DeleteMessageBatchRequest = {
          QueueUrl: taskUri,
          Entries: entries
        };
        promises.push(
          this.sqsClient(taskUri)
            .deleteMessageBatch(params)
            .promise()
        );
      }
    }

    // 실행
    const promisesChunk = ArrayUtils.chunk(promises, concurrency);
    const deletedIds: string[] = [];
    const failedResults: BatchResultErrorEntry[] = [];
    for (const promis of promisesChunk) {
      const bulkResults = await PromiseUtils.all(promis);

      // API 요청에 대한 에러가 발생한 경우
      if (bulkResults.failed.length) {
        throw bulkResults.failed[0];
      }

      for (const result of bulkResults.successful) {
        deletedIds.push(...result.Successful.map(v => v.Id));
        failedResults.push(...result.Failed);
      }
    }

    return {
      deletedIds,
      failedResults
    };
  }

  /**
   * 처리하지 못한 테스크를 반환하여 바로 다시 수신 가능한 상태로 변경
   *
   * < 자세히 >
   * Visibility Timeout 시간을 0 으로 설정하여 즉시 재수신 가능한 상태로 변경
   * 이를 통해 다시 수신한 큐의 Visibility Timeout 시간은 큐 발송시 설정된 기본값으로 복원됨
   */
  public async returnTasks<T extends Task>(
    taskCarriers: TaskCarrier<T>[],
    concurrency = BULK_CONCURRENCY_COUNT
  ): Promise<{ returnedIds: string[]; failedResults: BatchResultErrorEntry[] }> {
    const groupByUri: { [key: string]: TaskCarrier<T>[] } = {};
    for (const carrier of taskCarriers) {
      if (!groupByUri[carrier.task.taskUri()]) groupByUri[carrier.task.taskUri()] = [];
      groupByUri[carrier.task.taskUri()].push(carrier);
    }

    const promises: Promise<PromiseResult<SQS.Types.DeleteMessageBatchResult, AWSError>>[] = [];
    for (const taskUri of Object.keys(groupByUri)) {
      const carrierGroup = groupByUri[taskUri];
      const chunks = ArrayUtils.chunk(carrierGroup, 10);

      for (const chunk of chunks) {
        const entries: ChangeMessageVisibilityBatchRequestEntryList = [];
        for (const carrier of chunk) {
          entries.push({
            Id: carrier.messageId,
            ReceiptHandle: carrier.receiptHandle,
            VisibilityTimeout: 0
          });
        }

        const params: ChangeMessageVisibilityBatchRequest = {
          QueueUrl: taskUri,
          Entries: entries
        };
        promises.push(
          this.sqsClient(taskUri)
            .changeMessageVisibilityBatch(params)
            .promise()
        );
      }
    }

    // 실행
    const promisesChunk = ArrayUtils.chunk(promises, concurrency);
    const returnedIds: string[] = [];
    const failedResults: BatchResultErrorEntry[] = [];
    for (const promis of promisesChunk) {
      const bulkResults = await PromiseUtils.all(promis);

      // API 요청에 대한 에러가 발생한 경우
      if (bulkResults.failed.length) {
        throw bulkResults.failed[0];
      }

      for (const result of bulkResults.successful) {
        returnedIds.push(...result.Successful.map(v => v.Id));
        failedResults.push(...result.Failed);
      }
    }

    return {
      returnedIds,
      failedResults
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public async setTaskTimeout<T extends Task>(taskCarrier: TaskCarrier<T>, second: number): Promise<any> {
    const visibilityParams = {
      QueueUrl: taskCarrier.task.taskUri(),
      ReceiptHandle: taskCarrier.receiptHandle,
      VisibilityTimeout: second
    };
    return await this.sqsClient(taskCarrier.task.taskUri())
      .changeMessageVisibility(visibilityParams)
      .promise();
  }

  public async isCheckedIn<T extends Task>(taskCarrier: TaskCarrier<T>): Promise<boolean> {
    return (await this.redisClient.get(this.taskCheckInRedisKey(taskCarrier))) !== null;
  }

  // 처리중임을 마킹
  public async checkIn<T extends Task>(taskCarrier: TaskCarrier<T>, ttl = CHECK_IN_DEFAULT_TTL) {
    const res = await this.redisClient.set(this.taskCheckInRedisKey(taskCarrier), 1, 'EX', ttl, 'NX');
    if (res !== 'OK') {
      throw new DuplicatedCheckInError(`duplicated check-in, MessageId : ${taskCarrier.messageId}`);
    }
  }

  // 체크인 TTL 연장
  public async checkInExpire<T extends Task>(taskCarrier: TaskCarrier<T>, ttl = CHECK_IN_DEFAULT_TTL): Promise<boolean> {
    const res = await this.redisClient.expire(this.taskCheckInRedisKey(taskCarrier), ttl);
    return res === 1;
  }

  // 처리가 완료된 것으로 마킹, holdingTime 동안은 유지
  public async checkOut<T extends Task>(taskCarrier: TaskCarrier<T>, holdingSecond: number = CHECK_OUT_HOLDING_TTL) {
    if (holdingSecond) await this.redisClient.expire(this.taskCheckInRedisKey(taskCarrier), holdingSecond);
    else await this.redisClient.del(this.taskCheckInRedisKey(taskCarrier));
  }

  private taskCheckInRedisKey<T extends Task>(taskCarrier: TaskCarrier<T>): string {
    return `tm:${taskCarrier.task.constructor.name}:mid:${taskCarrier.messageId}`;
  }

  private get redisClient() {
    return RedisClient.host(REDIS_HOST);
  }

  private sqsClient(taskUri: string): SQS {
    const region = this.regionByTaskUri(taskUri);
    const client = this.sqsClientMap.get(region);
    if (client) return client;
    else {
      const newClient = new SQS({ region });
      this.sqsClientMap.set(region, newClient);
      return newClient;
    }
  }

  private regionByTaskUri(taskUri: string): string {
    const region = this.uriRegionCache.get(taskUri);
    if (region) return region;

    const match = taskUri.match(/^https:\/\/sqs\.([a-z0-9-]+)/);
    if (!match) throw new Error('unknown task uri format.');

    this.uriRegionCache.set(taskUri, match[1]);
    return match[1];
  }
}
