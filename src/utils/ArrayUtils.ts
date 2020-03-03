export default class ArrayUtils {
  static chunk<T>(arr: T[], size: number): T[][] {
    const chunkList = [];
    let index = 0;
    while (index < arr.length) {
      chunkList.push(arr.slice(index, size + index));
      index += size;
    }
    return chunkList;
  }
}
