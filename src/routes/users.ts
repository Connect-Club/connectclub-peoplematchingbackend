import express from 'express'

import neo4jClient from '../dao/neo4jClient'
import postgresClient from '../dao/postgresClient'
import logger from '../utils/Logger'

const usersRouter = express.Router()

usersRouter.get('/dummy', async (req, res) => {
  res.send({ok: 1})
})

usersRouter.get('/dashboardStats', async (req, res) => {
  const result = await neo4jClient.getMatchingStats()
  res.send({data: result})
})

usersRouter.post('/getUsers', async (req, res, next) => {
  const results = await neo4jClient.getUsers(
    req.body.limit,
    req.body.offset,
    req.body.orderBy,
    req.body.orderDir,
    req.body.searchTerm,
  )
  res.send({data: results, code: 200})
})

usersRouter.get('/getUserTokens', async (req, res, next) => {
  const results = await neo4jClient.getUserTokens(
    req.query.userId ? Number(req.query.userId) : 0,
  )
  res.send({data: results, code: 200})
})

usersRouter.get('/getTokens', async (req, res, next) => {
  try {
    const tokens = await neo4jClient.getTokens(
      req.query.limit ? Number(req.query.limit) : 100,
      req.query.lastValue ? Number(req.query.lastValue) : 0,
    )
    if (!tokens || tokens?.length === 0) {
      res.send({
        data: {
          items: [],
          totalValue: 0,
          lastValue: null,
        },
        code: 200,
      })
      return
    }

    const totalTokens = (await neo4jClient.getTotalTokens()) || 0
    const lastValue = (Number(req.query.lastValue) || 0) + tokens.length

    res.send({
      data: {
        items: tokens,
        totalValue: totalTokens,
        lastValue: lastValue < totalTokens ? lastValue : null,
      },
      code: 200,
    })
  } catch (e) {
    logger.error(e)
    res.send({code: 500, error: e})
  }
})

usersRouter.get('/getUserWithMatches', async (req, res, next) => {
  const results = await neo4jClient.getUserMatches(
    req.query.userId ? Number(req.query.userId) : 0,
  )
  // logger.debug(results)
  const dbUsers = await postgresClient.getShortUserProfiles(
    results?.matches.map((m) => {
      return m.userId
    }) || [],
  )
  results?.matches.forEach((r) => {
    const dbUser = dbUsers.find((dbu) => {
      return dbu.userId === r.userId
    })
    if (dbUser) {
      r.username = dbUser.username
      r.fullName = dbUser.displayName
      r.avatar = dbUser.avatar
      r.about = dbUser.about
    }
  })
  res.send({data: results, code: 200})
})

usersRouter.get('/getUserFollowRecommendations', async (req, res, next) => {
  try {
    const uids = await neo4jClient.getUserRecommendations(
      req.query.userId ? Number(req.query.userId) : 0,
      req.query.limit ? Number(req.query.limit) : 100,
      req.query.lastValue ? Number(req.query.lastValue) : 0,
    )
    if (!uids || uids?.length === 0) {
      res.send({
        data: [],
        code: 200,
        lastValue: null,
      })
      return
    }
    const users = await postgresClient.getFullUserProfiles(uids || [])
    const results: Array<Record<string, any>> = []
    uids?.forEach((uid) => {
      const us = users.find((u) => {
        return u.id === uid
      })
      if (us) {
        results.push(us)
      }
    })
    res.send({
      data: results,
      code: 200,
      lastValue: (Number(req.query.lastValue) || 0) + uids.length,
    })
  } catch (e) {
    logger.error(e)
    res.send({code: 500, error: e})
  }
})

usersRouter.get('/getUserClubsRecommendations', async (req, res, next) => {
  try {
    const clubIds = await neo4jClient.getClubsRecommendations(
      req.query.userId ? Number(req.query.userId) : 0,
      req.query.limit ? Number(req.query.limit) : 100,
      req.query.lastValue ? Number(req.query.lastValue) : 0,
    )
    if (!clubIds || clubIds?.length === 0) {
      res.send({
        data: [],
        code: 200,
        lastValue: null,
      })
      return
    }
    // const clubs = await postgresClient.getClubsByIds(clubIds)
    const results: Array<Record<string, any>> = []
    clubIds.forEach((uid) => {
      // const us = clubs.find((u) => {
      //   return u.id === uid
      // })
      results.push({
        id: uid,
      })
    })
    res.send({
      data: results,
      code: 200,
      lastValue: (Number(req.query.lastValue) || 0) + clubIds.length,
    })
  } catch (e) {
    logger.error(e)
    res.send({code: 500, error: e})
  }
})

usersRouter.get('/getUpcomingEventsRecommendations', async (req, res, next) => {
  try {
    const isSuperUser = await postgresClient.getShouldUserAlwaysSeeAllEvents(
      Number(req.query.userId),
    )
    const openEvents = (await postgresClient.getOnlineEventSchedules()).map(
      (rec) => {
        return rec.event_schedule_id
      },
    )
    const eventsIds = await neo4jClient.getUpcomingEventsRecommendations(
      req.query.userId ? Number(req.query.userId) : 0,
      req.query.limit ? Number(req.query.limit) : 100,
      req.query.lastValue ? Number(req.query.lastValue) : 0,
      isSuperUser,
      openEvents,
    )
    if (!eventsIds || eventsIds?.length === 0) {
      res.send({
        data: [],
        code: 200,
        lastValue: null,
      })
      return
    }
    const results: Array<Record<string, any>> = []
    eventsIds.forEach((uid) => {
      results.push({
        id: uid,
      })
    })
    res.send({
      data: results,
      code: 200,
      lastValue: (Number(req.query.lastValue) || 0) + eventsIds.length,
    })
  } catch (e) {
    logger.error(e)
    res.send({code: 500, error: e})
  }
})

usersRouter.get('/getCalendarEventsRecommendations', async (req, res, next) => {
  try {
    const isSuperUser = await postgresClient.getShouldUserAlwaysSeeAllEvents(
      Number(req.query.userId),
    )
    const openEvents = (await postgresClient.getOnlineEventSchedules()).map(
      (rec) => {
        return rec.event_schedule_id
      },
    )
    const eventsIds = await neo4jClient.getCalendarEventsRecommendations(
      req.query.userId ? Number(req.query.userId) : 0,
      req.query.limit ? Number(req.query.limit) : 100,
      req.query.lastValue ? Number(req.query.lastValue) : 0,
      isSuperUser,
      openEvents,
    )
    if (!eventsIds || eventsIds?.length === 0) {
      res.send({
        data: [],
        code: 200,
        lastValue: null,
      })
      return
    }
    const results: Array<Record<string, any>> = []
    eventsIds.forEach((uid) => {
      results.push({
        id: uid,
      })
    })
    res.send({
      data: results,
      code: 200,
      lastValue: (Number(req.query.lastValue) || 0) + eventsIds.length,
    })
  } catch (e) {
    logger.error(e)
    res.send({code: 500, error: e})
  }
})

export default usersRouter
