import "babel-polyfill";
import mysql from 'mysql';
import _ from 'lodash';

const sqlPromise = (connection, stmt, vars) => new Promise((resolve, reject) => connection.query(stmt, vars, (e, r) => e ? reject(e) : resolve(r)));

export default async (event, context, callback) => {
  const connection = mysql.createConnection({
    host: process.env.MY_SQL_HOST,
    port: process.env.MY_SQL_PORT,
    user: process.env.MY_SQL_USERNAME,
    password: process.env.MY_SQL_PASSWORD,
    database: process.env.MY_SQL_DATABASE,
    ssl: process.env.MY_SQL_SSL,
    supportBigNumbers: true,
    bigNumberStrings: true,
  });
  // try opening the connection
  try {
    await new Promise((resolve, reject) => connection.connect(e => e ? reject(e) : resolve()));
  } catch (e) {
    console.error(e);
    connection.destroy();
    callback(e);
  }
  try {
    const {
      targetThreshold,
      targetColumn,
      maxCommits,
      forcedId,
      datasetPartition,
      whichDataset
    } = event;
    console.log("datasetPartition",datasetPartition,"whichDataset",whichDataset,"targetColumn",targetColumn,"targetThreshold",targetThreshold,"maxCommits",maxCommits,"forcedId",forcedId,"n",event.n,"o",event.o);
    const createStatsTable = c => sqlPromise(c, 'CREATE TABLE IF NOT EXISTS github_stats(id INT PRIMARY KEY, stargazers_count_min INT, stargazers_count_max INT, forks_count_min INT, forks_count_max INT, watchers_count_min INT, watchers_count_max INT, subscribers_count_min INT, subscribers_count_max INT, deletions_min INT, deletions_max INT, additions_min INT, additions_max INT, test_deletions_min INT, test_deletions_max INT, test_additions_min INT, test_additions_max INT, author_date_min BIGINT, author_date_max BIGINT, committer_date_min BIGINT, committer_date_max BIGINT, repo_count BIGINT, commit_count BIGINT);'); // create stats table
    await createStatsTable(connection);
    let stats = await sqlPromise(connection, 'SELECT stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max, repo_count, commit_count FROM github_stats;');
    const statsTest = await sqlPromise(connection, 'SELECT COUNT(*) AS commit_count FROM commits;');
    if (stats.length === 0 || parseInt(stats[0].commit_count) !== parseInt(statsTest[0].commit_count)) {
      // generate the stats...takes a while
      await sqlPromise(connection, 'DROP TABLE IF EXISTS github_stats;')
      await createStatsTable(connection);
      const repoStats = await sqlPromise(connection, 'SELECT MIN(stargazers_count) AS stargazers_count_min, MAX(stargazers_count) AS stargazers_count_max, MIN(forks_count) AS forks_count_min, MAX(forks_count) AS forks_count_max, MIN(watchers_count) AS watchers_count_min, MAX(watchers_count) AS watchers_count_max, MIN(subscribers_count) AS subscribers_count_min, MAX(subscribers_count) AS subscribers_count_max, COUNT(*) AS repo_count FROM repos;');
      const commitStats = await sqlPromise(connection, 'SELECT MIN(deletions) AS deletions_min, MAX(deletions) AS deletions_max, MIN(additions) AS additions_min, MAX(additions) AS additions_max, MIN(test_deletions) AS test_deletions_min, MAX(test_deletions) AS test_deletions_max, MIN(test_additions) AS test_additions_min, MAX(test_additions) AS test_additions_max, COUNT(*) AS commit_count FROM commits;');
      const now = new Date().getTime();
      await sqlPromise(connection, `INSERT INTO github_stats (id, stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, repo_count, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max, author_date_min, author_date_max, committer_date_min, committer_date_max, commit_count) VALUES (0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE stargazers_count_min = ?, stargazers_count_max = ?, forks_count_min = ?, forks_count_max = ?, watchers_count_min = ?, watchers_count_max = ?, subscribers_count_min = ?, subscribers_count_max = ?, repo_count = ?, deletions_min = ?, deletions_max = ?, additions_min = ?, additions_max = ?, test_deletions_min = ?, test_deletions_max = ?, test_additions_min = ?, test_additions_max = ?, author_date_min = ?, author_date_max = ?, committer_date_min = ?, committer_date_max = ?, commit_count = ?;`,
        _.flatten(new Array(2).fill([
          repoStats[0].stargazers_count_min,
          repoStats[0].stargazers_count_max,
          repoStats[0].forks_count_min,
          repoStats[0].forks_count_max,
          repoStats[0].watchers_count_min,
          repoStats[0].watchers_count_max,
          repoStats[0].subscribers_count_min,
          repoStats[0].subscribers_count_max,
          repoStats[0].repo_count,
          commitStats[0].deletions_min,
          commitStats[0].deletions_max,
          commitStats[0].additions_min,
          commitStats[0].additions_max,
          commitStats[0].test_deletions_min,
          commitStats[0].test_deletions_max,
          commitStats[0].test_additions_min,
          commitStats[0].test_additions_max,
          new Date('2005-01-01T00:00:00Z').getTime(), // git was created in 2005, seems reasonable...
          now,
          new Date('2005-01-01T00:00:00Z').getTime(),
          now,
          commitStats[0].commit_count
        ])));
      stats = await sqlPromise(connection, 'SELECT stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, author_date_min, author_date_max, committer_date_min, committer_date_min, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max, repo_count, commit_count FROM github_stats;');
    }
    // takes a value, a min, and a max and normalizes the value with 0 mean and range between -1 and 1
    const normalize = (r, lo, hi) => (r - ((hi + lo) / 2)) / ((hi - lo) / 2);
    // functions that get a distinct value from an object
    const _distinct = $ => a => a[$];
    const distinctAuthorName = _distinct('author_name');
    const distinctCommitterName = _distinct('committer_name');
    const distinctAuthorEmail = _distinct('author_email');
    const distinctCommitterEmail = _distinct('committer_email');
    // looks at a 2-dimensionnal aray, for example [['a'],['b'],['c'],['d']], and evaluates which rows are distinct
    // based on function f for a range of rows i - n to i inclusive
    const distinctInGroup = (group, i, n, f) => _.uniqBy(group.slice(Math.max(0, i - n), i + 1), f).length;
    // used to split data into train, validate and test sets
    const truncatedPartition = (datasetPartition || '50_25_25').split('_').slice(0, 3).map(x => parseInt(x));
    const partitionSum = _.sum(truncatedPartition);
    const datasetTripartite = _.fromPairs(truncatedPartition.map((n, i) => [i === 0 ? 'train' : i === 1 ? 'validate' : 'test', n / partitionSum]));
    // function used to get n (number of datapoints) and o (offset in dataset)
    const _getNO = (n, o, hardLimit) => o >= hardLimit ? {
      n: 0,
      o
    } : o + n > hardLimit ? {
      n: hardLimit - o,
      o
    } : {
      n,
      o
    };
    // moves o to the correct position given the dataset we want (train, validate, test) and then calls _getNO
    const getNO = (n, o, partition, which) => which === 'train' ?
      _getNO(n, o, Math.floor(partition['train'] * parseInt(stats[0].repo_count))) :
      which === 'validate' ?
      _getNO(n, o + Math.floor(partition['train'] * parseInt(stats[0].repo_count)), Math.floor((partition['train'] + partition['validate']) * parseInt(stats[0].repo_count))) :
      which === 'test' ?
      _getNO(n, o + Math.floor((partition['train'] + partition['validate']) * parseInt(stats[0].repo_count)), parseInt(stats[0].repo_count)) :
      (() => {
        throw new Error('which must be train, validate or test')
      })();
    // get the target results
    const {
      n,
      o
    } = getNO(parseInt(event.n || 100), parseInt(event.o || 0), datasetTripartite, whichDataset || 'train');
    // this is useful if we ever want to inspect one particular repo
    const targetResults = forcedId ? await sqlPromise(connection, `SELECT id, ${targetColumn} FROM repos WHERE id = ?;`, [parseInt(forcedId)]) : await sqlPromise(connection, `SELECT id, ${targetColumn} FROM repos ORDER BY id ASC LIMIT ? OFFSET ?;`, [n, o]);
    if (targetResults.length === 0) {
      // our offset is too high or our n is 0, so we just return nothing
      callback(null, []);
    } else {
      // construct as a map for the target set with the repo id as the keys and the meeshkan-readable array as the values
      const targetMap = _.fromPairs(targetResults.map(x => [x.id, [parseInt(targetThreshold) <= 0 ? normalize(parseInt(x[targetColumn]), parseInt(stats[0][`${targetColumn}_min`]), parseInt(stats[0][`${targetColumn}_max`])) : parseInt(x[targetColumn]) < parseInt(targetThreshold) ? 0 : 1]]));
      // get the feature results
      const featureResults = await sqlPromise(connection, `SELECT repo_id, author_name, author_email, committer_name, committer_email, author_date, committer_date, additions, deletions, test_additions, test_deletions FROM commits WHERE ${targetResults.map(x => 'repo_id = ?').join(' OR ')};`, targetResults.map(x => x.id));
      // construct as a map for the feature set with the repo id as the keys and the meeshkan-readable array as the values
      const featureMap = _.fromPairs(Object.values(_.groupBy(featureResults, x => x.repo_id)).map(x => x.sort((a, b) => parseInt(a.author_date) - parseInt(b.author_date)))
        .map(group => group.slice(0, parseInt(maxCommits))).map(group => [group[0].repo_id, _.flatten(group.map((row, i) => [
          // the null thing is a hack...basically, if i is 1, it spits out null...need to figure out why...
          ...(event.authorHistory.split('_').map(j => parseInt(j)).map(j => normalize(Math.min(1, distinctInGroup(group, i, j, distinctAuthorName), distinctInGroup(group, i, j, distinctAuthorEmail)), 1, j))).map(x => x === null ? -1 : x),
          ...(event.committerHistory.split('_').map(j => parseInt(j)).map(j => normalize(Math.min(1, distinctInGroup(group, i, j, distinctCommitterName), distinctInGroup(group, i, j, distinctCommitterEmail)), 1, j))).map(x => x === null ? -1 : x),
          normalize(parseInt(row.author_date), parseInt(stats[0].author_date_min), parseInt(stats[0].author_date_max)) || 0,
          normalize(parseInt(row.committer_date), parseInt(stats[0].committer_date_min), parseInt(stats[0].committer_date_max)) || 0,
          normalize(parseInt(row.additions), parseInt(stats[0].additions_min), parseInt(stats[0].additions_max)) || 0,
          normalize(parseInt(row.deletions), parseInt(stats[0].deletions_min), parseInt(stats[0].deletions_max)) || 0,
          normalize(parseInt(row.test_additions), parseInt(stats[0].test_additions_min), parseInt(stats[0].test_additions_max)) || 0,
          normalize(parseInt(row.test_deletions), parseInt(stats[0].test_deletions_min), parseInt(stats[0].test_deletions_max)) || 0
        ]).concat(new Array(Math.max(0, parseInt(maxCommits) - group.length)).fill(new Array(12).fill(0.0))))]));
      // weaves the feature and target set together into something ingestible by meeshkan
      callback(null, Object.keys(targetMap).map(key => [featureMap[key] || new Array(100 * (6 + event.authorHistory.split('_').length + event.committerHistory.split('_').length)).fill(0.0), targetMap[key]]));
    }
  } catch (e) {
    console.error(e);
    callback(e);
  } finally {
    connection.destroy();
  }
}