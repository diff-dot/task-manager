export default class PromiseUtils {
  static async usleep(duration: number) {
    await new Promise(resolve => {
      setTimeout(() => {
        resolve();
      }, duration);
    });
  }

  static async sleep(duration: number) {
    await PromiseUtils.usleep(duration * 1000);
  }
  /**
   * 오류가 발생해도 전달받은 모든 promise 를 실행
   *
   * @param promises 프로미스 목록
   * @param enforce 중간에 오류 발생시 강제 실행 여부 포함
   */
  static async all<T>(promises: Promise<T>[]): Promise<{ successful: T[]; failed: Error[] }> {
    const successful: T[] = [];
    const failed: Error[] = [];
    for (const promise of promises) {
      try {
        successful.push(await promise);
      } catch (e) {
        failed.push(e);
      }
    }
    return { successful, failed };
  }
}
