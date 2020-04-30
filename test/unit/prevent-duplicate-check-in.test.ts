import { expect } from 'chai';
import { TaskManager, Task, TaskCarrier } from '../../src';

class DummyTask implements Task {
  taskDelay(): undefined {
    return undefined;
  }
  taskUri(): string {
    return 'https://sqs.us-east-2.amazonaws.com/00000/dummy-tasks';
  }
}

const taskManager = new TaskManager({
  redisHost: {
    host: '127.0.0.1',
    port: 6379
  }
});
const task = new DummyTask();
const taskCarrier = new TaskCarrier<DummyTask>();
taskCarrier.messageId = 'testMessageId';
taskCarrier.task = task;

describe('prevent-duplicate-check-in.test', async () => {
  before(async () => {
    await taskManager.checkOut(taskCarrier, 0);
  });

  it('Message ID 를 사용한 중복 체크인 방지', async () => {
    await taskManager.checkIn(taskCarrier);
    try {
      await taskManager.checkIn(taskCarrier);
      expect(true).to.be.false;
    } catch (e) {
      expect(true).to.be.true;
    }
  });
});
