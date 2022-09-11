export type TagType = Tag | ExtendedTag | BehaviorTag

export interface Tag {
  score: number
  id: string
  category: string
  layer?: string
}

export interface ExtendedTag extends Tag {
  interestName: string
  categoryName: string
}

export interface BehaviorTag extends Tag {
  interests: Array<string>
}

export type TagsGroupedByLayer = Array<{
  layer: string
  tags: Array<TagType & {nodeId: number}>
}>

export interface ClubTag {
  clubId: string
  interests: Array<string>
  createdAt: number
  isPublic: boolean
}

export interface ClubJoinTag extends ClubTag {
  role: string
}

export interface ClubRoleChange {
  clubId: string
  userId: number
  role: string
}
