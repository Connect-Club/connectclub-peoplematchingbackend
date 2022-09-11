import 'dotenv/config'

import express from 'express'
import logger from 'morgan'

import {Consumer} from './src/amqp'
import neo4jClient from './src/dao/neo4jClient'
import {UserBioToSkillsImportJob} from './src/jobs/userBioToSkillsImportJob'
import {UserClubsImportJob} from './src/jobs/userClubsImportJob'
import {UserEventActivitiesImportJob} from './src/jobs/userEventActivitiesImportJob'
import {UserEventsParticipantsImportJob} from './src/jobs/userEventsParticipantsImportJob'
import {UserGoalsImportJob} from './src/jobs/userGoalsImportJob'
import {UserIndustriesImportJob} from './src/jobs/userIndustriesImportJob'
import {UserInterestsImportJob} from './src/jobs/userInterestsImportJob'
import {UserScheduledEventsImportJob} from './src/jobs/userScheduledEventsImportJob'
import {UsersImportJob} from './src/jobs/usersImportJob'
import {UserSkillsImportJob} from './src/jobs/userSkillsImportJob'
// import {UserWalletsImportJob} from './src/jobs/userWalletsImportJob'
import tagsRouter from './src/routes/tags'
import usersRouter from './src/routes/users'
import scheduler from './src/scheduler/scheduler'

// dotenv.config()
const app = express()
app.use(express.json())
app.use(logger('combined'))
app.use(express.urlencoded({extended: false}))
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*')
  res.header(
    'Access-Control-Allow-Headers',
    'Origin, X-Requested-With, Content-Type, Accept',
  )
  next()
})
app.use('/users', usersRouter)
app.use('/tags', tagsRouter)

const port = process.env.BACKEND_PORT || 8000

neo4jClient.bootstrap().then((noUsers) => {
  if (noUsers) {
    console.log('No users in NEO4J. starting sync process')
    new UsersImportJob()
      .doImport()
      .then(() => {
        return new UserInterestsImportJob().doImport()
      })
      .then(() => {
        return new UserSkillsImportJob().doImport()
      })
      .then(() => {
        return new UserIndustriesImportJob().doImport()
      })
      .then(() => {
        return new UserGoalsImportJob().doImport()
      })
      .then(() => {
        return new UserEventsParticipantsImportJob().doImport()
      })
      .then(() => {
        return new UserEventActivitiesImportJob().doImport()
      })
      .then(() => {
        return new UserClubsImportJob().doImport()
      })
      .then(() => {
        return new UserBioToSkillsImportJob().doImport()
      })
      .then(() => {
        return new UserScheduledEventsImportJob().doImport()
      })
  }
  // new UserWalletsImportJob().doImport().then()
  // Listen to AMQP
  Consumer.listen()

  if (process.env.NODE_ENV === 'production') {
    scheduler.start()
  }

  app.listen(port, () => {
    console.log(`⚡️[server]: listening on port ${port}`)
  })
})

// Exports for testing purposes.
module.exports = app
