import { Expose } from 'class-transformer';
import { IsString } from 'class-validator';
import { Task } from '../../../src';

export class DummyTask implements Task {
  @Expose()
  @IsString()
  dummyId: string;

  constructor(args?: { dummyId: string }) {
    if (args) {
      this.dummyId = args.dummyId;
    }
  }

  taskUri(): string {
    return `https://sqs.ap-northeast-2.amazonaws.com/${process.env.AWS_ACCOUNT_ID}/d-test`;
  }

  taskDelay(): number {
    return 0;
  }
}
