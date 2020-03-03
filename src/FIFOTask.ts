import { Task } from './Task';

export interface FIFOTask extends Task {
  taskGroupId(): string;
  taskDeduplicationId(): string | undefined;
}
