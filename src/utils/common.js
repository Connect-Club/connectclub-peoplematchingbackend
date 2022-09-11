// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export function removeEmoji(str) {
  return str.replace(
    /[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2580-\u27BF]|\uD83E[\uDD10-\uDDFF]/g,
    '',
  )
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

export const stringEmpty = (str) => {
  return !str || str.length === 0
}

export const chunkArray = (array, chunkSize) => {
  return Array.from({length: Math.ceil(array.length / chunkSize)}, (_, index) =>
    array.slice(index * chunkSize, (index + 1) * chunkSize),
  )
}
