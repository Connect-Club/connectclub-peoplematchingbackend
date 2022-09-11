CALL apoc.periodic.repeat(
  "calculate person matches",
  "
    CALL apoc.periodic.iterate(
  \"
    MATCH (u:User {state: 'verified'})
    OPTIONAL MATCH (u)-[:HAS_TAG|SPOKE_AT_EVENT|PARTICIPATED_IN_EVENT|HAS_NFT|MEMBER_AT_CLUB|OWNER_AT_CLUB]->()<-[]-(muser:User {state: 'verified'})
    WHERE
      id(u) <> id(muser)
      AND NOT (u)-[:FOLLOWS]->(muser)
      AND NOT (u)-[:HAS_PHONE_CONTACT]->(muser)
      AND (muser.isTester is null OR muser.isTester = false)
      AND size(apoc.coll.intersection(u.languages, muser.languages)) > 0
    WITH u, collect(distinct(muser.userId)) as col1
    OPTIONAL MATCH (u)-[:HAS_PHONE_CONTACT]->(u2:User {state: 'verified'})
    WHERE NOT (u)-[:FOLLOWS]-(u2)
    WITH u, col1, collect(distinct(u2.userId)) as col2
    RETURN u, size(apoc.coll.union(col1, col2)) as matches
    \",
  \"SET u.totalMatches = matches\",
  {batchSize:50, parallel:true})
  ",
   12*3600
);