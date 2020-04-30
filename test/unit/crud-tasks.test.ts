import { expect } from 'chai';
import { TaskManager, TaskCarrier } from '../../src';
import { DummyTask } from './tasks/DummyTask';
import { PromiseUtils } from '../../src/utils';

const TASK_BULK_SIZE = 100;

const taskManager = new TaskManager({
  redisHost: {
    host: '127.0.0.1',
    port: 6379
  }
});
let tasks: TaskCarrier<DummyTask>[] = [];

describe('crawler > add-many-tasks', async () => {
  it('단일 테스크 생성', async () => {
    const res = await taskManager.addTask(new DummyTask({ dummyId: 'dummyId' }));
    expect(res).to.be.string;
  });

  it('테스크 대량 생성', async () => {
    const arr: number[] = [];
    for (let i = 0; i < TASK_BULK_SIZE; i++) {
      arr.push(i);
    }
    const tasks: DummyTask[] = arr.map(v => new DummyTask({ dummyId: v.toString() }));

    const res = await taskManager.addTasks(tasks);
    expect(res.sendedIds.length).to.be.eq(TASK_BULK_SIZE);
  });

  it('테스크 수신', async () => {
    await PromiseUtils.usleep(1500);
    tasks = await taskManager.receiveTasks(DummyTask);
    expect(tasks.length > 0).to.be.true;
  });

  it('테스크 반환', async () => {
    const res = await taskManager.returnTasks(tasks);
    expect(res.returnedIds.length).to.be.eq(tasks.length);
  });

  it('테스크 재 수신', async () => {
    await PromiseUtils.usleep(500);
    tasks = await taskManager.receiveTasks(DummyTask);
    expect(tasks.length > 0).to.be.true;
  });

  it('테스크 삭제', async () => {
    const res = await taskManager.deleteTasks(tasks);
    expect(res.deletedIds.length).to.be.eq(tasks.length);
  });
});
