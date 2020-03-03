export interface Task {
  taskUri(): string;
  taskDelay(): number | undefined;
}
