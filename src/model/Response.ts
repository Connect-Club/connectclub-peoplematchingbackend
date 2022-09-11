export interface Response<T> {
  data?: T
  code: number
  error?: string
}
