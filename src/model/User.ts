import {BehaviorTag, ExtendedTag, Tag, TagType} from './Tag'

export interface User {
  userId: number
  username: string
  fullName: string
  skills: Array<Tag>
  industry: Array<Tag>
  professionalGoals: Array<Tag>
  socialGoals: Array<Tag>
  lookingFor: Array<Tag>
  generalInterests: Array<ExtendedTag>
  behavior: {participated: number; spoke: number}
}

export interface Match {
  userId: number
  avatar: string | null
  about: string | null
  username: string
  fullName: string
  score: number
  matchedTags: Array<TagType>
}

export interface UserWithMatches extends User {
  matches: Array<Match>
}

export interface UserListRecord {
  username: string
  fullName: string
  numOfTag: number
  matches: number
}

export interface UserListWithTotal {
  userRecords: Array<UserListRecord>
  total: number
}

export interface UserToken {
  name: string
  tokenId: string
  image: string
  description: string | null
}
