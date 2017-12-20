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
    await sqlPromise(connection, 'CREATE TABLE IF NOT EXISTS github_stats(id INT PRIMARY KEY, stargazers_count_min INT, stargazers_count_max INT, forks_count_min INT, forks_count_max INT, watchers_count_min INT, watchers_count_max INT, subscribers_count_min INT, subscribers_count_max INT, deletions_min INT, deletions_max INT, additions_min INT, additions_max INT, test_deletions_min INT, test_deletions_max INT, test_additions_min INT, test_additions_max INT, author_date_min BIGINT, author_date_max BIGINT, committer_date_min BIGINT, committer_date_max BIGINT);'); // create stats table
    let stats = await sqlPromise(connection, 'SELECT stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max FROM github_stats;');
    if (stats.length === 0 || event.refreshStats) {
      // generate the stats...takes a while
      const repoStats = await sqlPromise(connection, 'SELECT MIN(stargazers_count) AS stargazers_count_min, MAX(stargazers_count) AS stargazers_count_max, MIN(forks_count) AS forks_count_min, MAX(forks_count) AS forks_count_max, MIN(watchers_count) AS watchers_count_min, MAX(watchers_count) AS watchers_count_max, MIN(subscribers_count) AS subscribers_count_min, MAX(subscribers_count) AS subscribers_count_max FROM repos;');
      const commitStats = await sqlPromise(connection, 'SELECT MIN(deletions) AS deletions_min, MAX(deletions) AS deletions_max, MIN(additions) AS additions_min, MAX(additions) AS additions_max, MIN(test_deletions) AS test_deletions_min, MAX(test_deletions) AS test_deletions_max, MIN(test_additions) AS test_additions_min, MAX(test_additions) AS test_additions_max FROM commits;');
      const now = new Date().getTime();
      await sqlPromise(connection, 'INSERT INTO github_stats (id, stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max, author_date_min, author_date_max, committer_date_min, committer_date_max) VALUES (0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE stargazers_count_min = ?, stargazers_count_max = ?, forks_count_min = ?, forks_count_max = ?, watchers_count_min = ?, watchers_count_max = ?, subscribers_count_min = ?, subscribers_count_max = ?, deletions_min = ?, deletions_max = ?, additions_min = ?, additions_max = ?, test_deletions_min = ?, test_deletions_max = ?, test_additions_min = ?, test_additions_max = ?, author_date_min = ?, author_date_max = ?, committer_date_min = ?, committer_date_max = ?;',
        _.flatten(new Array(2).fill([
          repoStats[0].stargazers_count_min,
          repoStats[0].stargazers_count_max,
          repoStats[0].forks_count_min,
          repoStats[0].forks_count_max,
          repoStats[0].watchers_count_min,
          repoStats[0].watchers_count_max,
          repoStats[0].subscribers_count_min,
          repoStats[0].subscribers_count_max,
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
          now
        ])));
      stats = await sqlPromise(connection, 'SELECT stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, author_date_min, author_date_max, committer_date_min, committer_date_min, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max FROM github_stats;');
    }
    // some setup
    const normalize = (r, lo, hi) => (r - ((hi + lo) / 2)) / ((hi - lo) / 2);
    const {
      targetThreshold,
      targetColumn,
      maxCommits
    } = event;
    const distinctAuthorName = a => a.author_name;
    const distinctCommitterName = a => a.committer_name;
    const distinctAuthorEmail = a => a.author_email;
    const distinctCommitterEmail = a => a.committer_email;
    const distinctInGroup = (group, i, n, f) => _.uniqBy(group.slice(Math.max(0, i - n), i + 1), f).length;
    const getNO = (n, o, hardLimit) => o >= hardLimit ? {
      n: 0,
      o
    } : o + n > hardLimit ? {
      n: hardLimit - o,
      o
    } : {
      n,
      o
    };
    // get the target results
    const {
      n,
      o
    } = getNO(parseInt(event.n || 100), parseInt(event.o || 0), parseInt(event.hardLimit));
    const targetResults = await sqlPromise(connection, `SELECT id, ${targetColumn} FROM repos ORDER BY id ASC LIMIT ? OFFSET ?;`, [n, o]);
    // construct as a meeshkan-readable map
    const targetMap = _.fromPairs(targetResults.map(x => [x.id, [parseInt(targetThreshold) <= 0 ? normalize(parseInt(x[targetColumn]), parseInt(stats[0][`${targetColumn}_min`]), parseInt(stats[0][`${targetColumn}_max`])) : parseInt(x[targetColumn]) < parseInt(targetThreshold) ? 0 : 1]]));
    // get the feature results
    const featureResults = await sqlPromise(connection, `SELECT repo_id, author_name, author_email, committer_name, committer_email, author_date, committer_date, additions, deletions, test_additions, test_deletions FROM commits WHERE ${targetResults.map(x => 'repo_id = ?').join(' OR ')};`, targetResults.map(x => x.id));
    // construct as a meeshkan-readable map.
    const featureMap = _.fromPairs(Object.values(_.groupBy(featureResults, x => x.repo_id)).map(x => x.sort((a, b) => parseInt(a.author_date) - parseInt(b.author_date)))
      .map(group => group.slice(0, parseInt(maxCommits))).map(group => [group[0].repo_id, _.flatten(group.map((row, i) => [
        ...(event.authorHistory.split('_').map(j => parseInt(j)).map(j => normalize(Math.min(1, distinctInGroup(group, i, j, distinctAuthorName), distinctInGroup(group, i, j, distinctAuthorEmail)), 1, j))),
        ...(event.committerHistory.split('_').map(j => parseInt(j)).map(j => normalize(Math.min(1, distinctInGroup(group, i, j, distinctCommitterName), distinctInGroup(group, i, j, distinctCommitterEmail)), 1, j))),
        normalize(parseInt(row.author_date), parseInt(stats[0].author_date_min), parseInt(stats[0].author_date_max)) || 0,
        normalize(parseInt(row.committer_date), parseInt(stats[0].committer_date_min), parseInt(stats[0].committer_date_max)) || 0,
        normalize(parseInt(row.additions), parseInt(stats[0].additions_min), parseInt(stats[0].additions_max)) || 0,
        normalize(parseInt(row.deletions), parseInt(stats[0].deletions_min), parseInt(stats[0].deletions_max)) || 0,
        normalize(parseInt(row.test_additions), parseInt(stats[0].test_additions_min), parseInt(stats[0].test_additions_max)) || 0,
        normalize(parseInt(row.test_deletions), parseInt(stats[0].test_deletions_min), parseInt(stats[0].test_deletions_max)) || 0
      ]).concat(new Array(Math.max(0, parseInt(maxCommits) - group.length)).fill(new Array(12).fill(0.0))))]));
    callback(null, Object.keys(targetMap).map(key => [featureMap[key] || new Array(100 * (6 + event.authorHistory.split('_').length + event.committerHistory.split('_').length)).fill(0.0), targetMap[key]]));
  } catch (e) {
    console.error(e);
    callback(e);
  } finally {
    connection.destroy();
  }
}