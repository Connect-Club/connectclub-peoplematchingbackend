import express from 'express'

import neo4jClient from '../dao/neo4jClient'

const tagsRouter = express.Router()

tagsRouter.post('/getTagsGroupedByLayer', async (req, res) => {
  const results = await neo4jClient.getTagsGroupedByLayer()
  res.send({data: results, code: 200})
})

tagsRouter.post('/updateTagsScore', async (req, res) => {
  const results = await neo4jClient.updateTagsScore(req.body)
  res.send({data: results, code: 200})
})

export default tagsRouter
