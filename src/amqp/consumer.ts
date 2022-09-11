import moment from 'moment'

import moralisClient from '../api/moralisClient'
import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import logger from '../utils/Logger'
import {MessageBroker} from './MessageBroker'

type UserDataProps = {
  ownerId: number
  username: string
  firstName: string
  lastName: string
  languages: Array<string>
  recommended_for_following_priority?: number
  state: string
  isTester?: boolean
}

type InterestTag = {
  interestId: number
  groupId: number
  interestName: string
  groupName: string
}

type UserBehaveProps = {
  id: number
  videoRoomId?: number
  ownerId: number
  interest_id: number[]
}

type UserFollowsProps = {
  ownerId: number
  userId: number
}

type UserContactsUpdatedProps = {
  ownerId: number
  userIds: number[]
}

type UserClubJoinProps = {
  ownerId: number
  clubId: string
  role?: string
}

type UserClubRoleChangeProps = {
  ownerId: number
  clubId: string
  role: string
}

type ClubInterestChangeProps = {
  clubId: string
  interests: Array<string>
}

type UserEventScheduled = {
  ownerId: number
  id: string
  interest_id: Array<number>
  is_private?: boolean
}

export const listen = (): void => {
  const amqp = new MessageBroker()
  amqp
    .connect()
    .then((broker) => {
      broker
        .subscribe(
          {
            name: 'matching',
            type: 'fanout',
            routingKey: '',
            queue: 'matching',
          },
          async (msg, ack, nack) => {
            const rawContent = msg?.content.toString()
            if (rawContent) {
              const message = JSON.parse(rawContent)
              if (message.data) {
                logger.debug('Got message', message)
                // Add/update user
                if (
                  message.id === 'userAdded' ||
                  message.id === 'userModified'
                ) {
                  const result = await onUserModified(message.data)
                  result ? ack() : nack()
                  return
                }

                // Interests | industries | skills | goals modified
                if (
                  [
                    'userGIModified',
                    'userIndustriesAmend',
                    'userSkillsAmend',
                    'userGoalsAmend',
                  ].includes(message.id)
                ) {
                  const result = await onInterestsModified(message)
                  result ? ack() : nack()
                  return
                }

                // Behavior actions
                if (
                  [
                    'userMeetingParticipate',
                    'userMeetingStageJoin',
                    'userMeetingRaiseHand',
                  ].includes(message.id)
                ) {
                  const result = await onUserBehave(message)
                  result ? ack() : nack()
                  return
                }

                // User club join
                if (message.id === 'userClubJoin') {
                  const result = await onUserClubJoin(message.data)
                  result ? ack() : nack()
                  return
                }

                // User club leave
                if (message.id === 'userLeaveFromClub') {
                  const result = await onUserClubLeave(message.data)
                  result ? ack() : nack()
                  return
                }

                // User club role change
                if (message.id === 'userClubRoleChange') {
                  const result = await onUserClubRoleChange(message.data)
                  result ? ack() : nack()
                  return
                }

                // User scheduled event
                if (message.id === 'userMeetingScheduled') {
                  const result = await onUserScheduledEvent(message.data)
                  result ? ack() : nack()
                  return
                }

                // User schedule event canceled
                if (message.id === 'eventScheduleRemoved') {
                  const result = await onUserScheduleRemovedEvent(message.data)
                  result ? ack() : nack()
                  return
                }

                // Scheduled event updated
                if (message.id === 'meetingScheduleUpdated') {
                  const result = await onScheduledUpdatedEvent(message.data)
                  result ? ack() : nack()
                  return
                }

                // User club interests change
                if (message.id === 'clubInterestChange') {
                  const result = await onClubInterestChange(message.data)
                  result ? ack() : nack()
                  return
                }

                // User followed user
                if (message.id === 'userFollowAdd') {
                  const result = await onUserFollows(message.data)
                  result ? ack() : nack()
                  return
                }

                // User unfollowed user
                if (message.id === 'userFollowRemove') {
                  const result = await onUserFollows(message.data, false)
                  result ? ack() : nack()
                  return
                }

                // User contacts from device updated
                if (message.id === 'userContactsUpdated') {
                  const result = await onUserContactsUpdated(message.data)
                  result ? ack() : nack()
                  return
                }

                // User wallet associated
                if (message.id === 'userWalletAdded') {
                  const result = await onUserWalletAdded(message.data)
                  result ? ack() : nack()
                  return
                }

                // User wallet removed
                if (message.id === 'userWalletRemoved') {
                  const result = await onUserWalletRemoved(message.data)
                  result ? ack() : nack()
                  return
                }

                // Club privacy changed
                if (message.id === 'clubSetPublic') {
                  const result = await onClubPrivacyChanged(message.data)
                  result ? ack() : nack()
                  return
                }
                logger.warn(
                  'UNKNOWN MESSAGE RECEIVED:',
                  JSON.stringify(message),
                )
                nack()
              }
            } else {
              ack()
            }
          },
        )
        .catch((e) => {
          logger.error(e)
          throw e
        })
    })
    .catch((e) => {
      logger.error(e)
      throw e
    })
}

const onUserModified = async (data: UserDataProps) => {
  if (data.ownerId) {
    return await neo4jClient.mergeUsers([
      {
        userId: data.ownerId,
        username: data.username,
        fullName: `${data.firstName} ${data.lastName}`.trim(),
        followedUsers: [],
        phoneContactsUsers: [],
        languages: data.languages,
        recommended_for_following_priority:
          data.recommended_for_following_priority || null,
        state: data.state,
        isTester: data.isTester || false,
      },
    ])
  } else {
    logger.warn('AMQP error. Consumer. onUserAdded: empty ownerId')
    return true
  }
}

const onInterestsModified = async (message: {
  id: string
  data: Record<string, any>
}) => {
  if (message.data.ownerId) {
    let tags = []
    let layer = ''
    let replaceTags: boolean | {replaceByCategory: string} = false

    // General interests modified
    if (message.id === 'userGIModified' && message?.data?.interests) {
      tags = message.data.interests.map((tag: InterestTag) => ({
        score: 1,
        id: tag.interestId.toString(),
        category: 'gen_interest',
        interestName: tag.interestName,
        categoryName: 'General interest',
      }))
      layer = 'general_layer'
      replaceTags = true
    }

    // User updated industries
    if (message.id === 'userIndustriesAmend' && message?.data?.industries) {
      const category = 'industry'
      tags = message.data.industries.map(
        (tag: {industryId: number; industryName: string}) => ({
          score: 1,
          id: tag.industryId.toString(),
          category: category,
          interestName: tag.industryName,
          categoryName: '',
        }),
      )
      layer = 'professional_layer'
      replaceTags = {replaceByCategory: category}
    }

    // User updated goals
    if (message.id === 'userGoalsAmend' && message?.data?.goals) {
      const category = 'goals'
      tags = message.data.goals.map(
        (tag: {goalId: number; goalName: string}) => ({
          score: 1,
          id: tag.goalId.toString(),
          category: 'goals',
          interestName: tag.goalName,
          categoryName: '',
        }),
      )
      layer = 'goals_layer'
      replaceTags = {replaceByCategory: category}
    }

    // User updated skills
    if (message.id === 'userSkillsAmend' && message?.data?.skills) {
      const category = 'skills'
      tags = message.data.skills.map(
        (tag: {skillId: number; skillName: string}) => ({
          score: 1,
          id: tag.skillId.toString(),
          category: 'skills',
          interestName: tag.skillName,
          categoryName: '',
        }),
      )
      layer = 'professional_layer'
      replaceTags = {replaceByCategory: category}
    }

    return await neo4jClient.mergeUserTags(
      message.data.ownerId,
      tags,
      layer,
      replaceTags,
    )
  } else {
    logger.warn(
      'AMQP error. Consumer. onInterestsModified: empty ownerId',
      message,
    )
    return true
  }
}

const onUserBehave = async (msg: {id: string; data: UserBehaveProps}) => {
  if (msg.id === 'userMeetingRaiseHand') {
    return true // for now
  }
  const clubId = await postgresClient.getClubForEvent(msg.data.id.toString())
  let success = await neo4jClient.mergeUserBehaviorActivity([
    {
      userId: msg.data.ownerId,
      activities: [
        {
          score: msg.id === 'userMeetingParticipate' ? 1 : 3,
          id: msg.data.id.toString(),
          category: msg.id,
          interests: msg.data.interest_id.map((i) => {
            return i.toString()
          }),
        },
      ],
    },
  ])
  if (!success) {
    return false
  }
  if (clubId) {
    success = await neo4jClient.mergeClubsWithEvents([
      {clubId: clubId, events: [msg.data.id]},
    ])
  }
  return success
}

const onUserScheduledEvent = async (data: Record<string, any>) => {
  const eventSchedule = await postgresClient.getEventSchedule(data.id)
  if (!eventSchedule) {
    logger.warn('EventSchedule not found in postgres', data.id)
    return false
  }
  return await neo4jClient.mergeUserEventSchedule([eventSchedule])
}

const onUserScheduleRemovedEvent = async (data: Record<string, any>) => {
  return await neo4jClient.removeEventSchedule(data)
}

const onScheduledUpdatedEvent = async (data: Record<string, any>) => {
  const eventSchedule = await postgresClient.getEventSchedule(data.id)
  if (!eventSchedule) {
    return true
  }
  return await neo4jClient.updateEventSchedule(eventSchedule)
}

const onUserClubJoin = async (data: UserClubJoinProps) => {
  if (!data.role) {
    // skip for now
    return true
  }
  const club = await postgresClient.getClubById(data.clubId)
  if (!club) {
    // weird!
    logger.error('club not found by given id', data.clubId)
    return true
  }
  return await neo4jClient.mergeUserClubs([
    {
      userId: data.ownerId,
      clubs: [
        {
          clubId: data.clubId,
          createdAt: club.createdAt,
          interests: club.interests || [],
          isPublic: club.isPublic === undefined ? false : club.isPublic,
          role: data.role,
        },
      ],
    },
  ])
}

const onUserClubLeave = async (data: UserClubJoinProps) => {
  const club = await postgresClient.getClubById(data.clubId)
  if (!club) {
    // weird!
    logger.error('club not found by given id', data.clubId)
    return true
  }
  return await neo4jClient.userLeaveClub(data.ownerId, data.clubId)
}

const onUserClubRoleChange = async (
  data: UserClubRoleChangeProps,
): Promise<boolean> => {
  return await neo4jClient.userClubRoleChange({
    clubId: data.clubId,
    userId: data.ownerId,
    role: data.role,
  })
}

const onClubInterestChange = async (
  data: ClubInterestChangeProps,
): Promise<boolean> => {
  return await neo4jClient.clubInterestsChange({
    clubId: data.clubId,
    interests: data.interests,
  })
}

const onClubPrivacyChanged = async (
  data: Record<string, any>,
): Promise<boolean> => {
  return await neo4jClient.clubPrivacyChange({
    clubId: data.clubId,
    isPublic: !data.isPrivate,
  })
}

const onUserFollows = async (data: UserFollowsProps, add = true) => {
  if (data.ownerId && data.userId) {
    if (add) {
      return await neo4jClient.addUserFollows(data.ownerId, data.userId)
    } else {
      return await neo4jClient.removeUserFollows(data.ownerId, data.userId)
    }
  } else {
    logger.warn(
      'AMQP error. Consumer. onUserFollows: empty userId or ownerId',
      data,
    )
    return true
  }
}

const onUserContactsUpdated = async (data: UserContactsUpdatedProps) => {
  if (data.ownerId && data.userIds) {
    const userIds = data.userIds.filter((uid) => {
      return uid !== data.ownerId
    })
    if (userIds.length > 0) {
      return await neo4jClient.updateDeviceContactsRelation(
        data.ownerId,
        userIds,
      )
    }
    return true
  } else {
    logger.warn(
      'AMQP error. Consumer. onUserContactsUpdated: empty ownerId or userIds',
      data,
    )
    return true
  }
}

const onUserWalletAdded = async (data: Record<string, any>) => {
  let success = await neo4jClient.mergeUserWallet(
    data.ownerId,
    data.wallet,
    moment().utc().unix(),
  )
  if (!success) {
    return false
  }
  const tokens = await moralisClient.getNftsByWallet(data.wallet)
  if (tokens.length > 0) {
    success = await neo4jClient.mergeUserNfts(data.ownerId, tokens)
  }
  return success
}

const onUserWalletRemoved = async (data: Record<string, any>) => {
  return await neo4jClient.removeUserWallet(data.ownerId)
}

const isNumber = (n: string | number): boolean =>
  !isNaN(parseFloat(String(n))) && isFinite(Number(n))
