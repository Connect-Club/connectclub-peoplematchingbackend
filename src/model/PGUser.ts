import {Integer} from 'neo4j-driver'

export interface PGUserBase {
  userId: number
  username: string
  fullName: string
  state: string
  followedUsers: Array<number | null>
  phoneContactsUsers: Array<number | null>
  languages: Array<string>
  recommended_for_following_priority: number | Integer | null
  isTester: boolean
}
