import { Task } from './Task';

export class TaskCarrier<T extends Task> {
  messageId: string;
  receiptHandle: string;
  task: T;
}
