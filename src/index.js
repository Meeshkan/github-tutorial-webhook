import "babel-polyfill";
import mysql from 'mysql';
import _ from 'lodash';

const sqlPromise = (connection, stmt, vars) => new Promise((resolve, reject) => connection.query(stmt, vars, (e, r) => e ? reject(e) : resolve(r)));

export default async () => {
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
    if (stats.length === 0) {
      // generate the stats...takes a while
      const repoStats = await sqlPromise(connection, 'SELECT MIN(stargazers_count) AS stargazers_count_min, MAX(stargazers_count) AS stargazers_count_max, MIN(forks_count) AS forks_count_min, MAX(forks_count) AS forks_count_max, MIN(watchers_count) AS watchers_count_min, MAX(watchers_count) AS watchers_count_max, MIN(subscribers_count) AS subscribers_count_min, MAX(subscribers_count) AS subscribers_count_max FROM repos;');
      const commitStats = await sqlPromise(connection, 'SELECT MIN(deletions) AS deletions_min, MAX(deletions) AS deletions_max, MIN(additions) AS additions_min, MAX(additions) AS additions_max, MIN(test_deletions) AS test_deletions_min, MAX(test_deletions) AS test_deletions_max, MIN(test_additions) AS test_additions_min, MAX(test_additions) AS test_additions_max FROM repos;');
      const now = new Date().getTime();
      await sqlPromise(connection, 'INSERT INTO github_stats (stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max, author_date_min, author_date_max, committer_date_min, committer_date_max) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);', [
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
      ]);
      stats = await sqlPromise(connection, 'SELECT stargazers_count_min, stargazers_count_max, forks_count_min, forks_count_max, watchers_count_min, watchers_count_max, subscribers_count_min, subscribers_count_max, deletions_min, deletions_max, additions_min, additions_max, test_deletions_min, test_deletions_max, test_additions_min, test_additions_max FROM github_stats;');
    }
    // some setup
    const normalize = (r, lo, hi) => (r - ((hi + lo) / 2)) / ((hi - lo) / 2);
    const targetColumn = event.useStars ? 'stargazers_count' : event.useForks ? 'forks_count' : event.useWatchers ? 'watchers_count' : event.useSubscribers ? 'subscribers_count' : 'stargazers_count';
    const distinctAuthorName = a => a.author_name;
    const distinctCommitterName = a => a.committer_name;
    const distinctAuthorEmail = a => a.author_email;
    const distinctCommitterEmail = a => a.committer_email;
    const distinctInGroup = (group, i, n, f) => _.uniqBy(group.slice(i - n, i), f);
    const getNO = (n, o, hardLimit) => o >= hardLimit ? { n: 0, o } : o + n > hardLimit ? { n: hardLimit - o, o }  : { n, o };
    // get the target results
    const {
      n,
      o
    } = getNO(parseInt(event.n || 100), parseInt(event.o || 0), parseInt(event.hardLimit));
    const targetResults = await sqlPromise(connection, `SELECT id, ${targetColumn} FROM repos ORDER BY id OFFSET ? LIMIT ?;`, [o, n]);
    // construct as a meeshkan-readable map
    const targetMap = _.fromPairs(targetResults.map(x => [x.id, [x[targetColumn]]]));
    // get the feature results
    const featureResults = await sqlPromise(connection, `SELECT repo_id, author_name, author_email, committer_name, committer_email, author_date, committer_date, additions, deletions, test_additions, test_deletions FROM commits WHERE ${targetResults.map(x => 'repo_id = ?').join(' OR ')};`, targetResults.map(x => x.id));
    // construct as a meeshkan-readable map. 12 features per commit x 100 commits deep = 1200 features per row
    const featureMap = _.fromPairs(Object.values(_.groupBy(featureResults, x => x.repo_id)).map(x => x.sort((a, b) => parseInt(a.author_date) - parseInt(b.author_date)))
      .map(group => [group[0].repo_id, _.flatten(group.map((row, i) => [
        normalize(Math.min(distinctInWindow(group, i, 3, distinctAuthorName), distinctInWindow(group, i, 3, distinctAuthorEmail)), 1, 3), // distinct authors 3 deep
        normalize(Math.min(distinctInWindow(group, i, 15, distinctAuthorName), distinctInWindow(group, i, 15, distinctAuthorEmail)), 1, 15), // distinct authors 15 deep
        normalize(Math.min(distinctInWindow(group, i, 100, distinctAuthorName), distinctInWindow(group, i, 100, distinctAuthorEmail)), 1, 100), // distinct authors 100 deep
        normalize(Math.min(distinctInWindow(group, i, 3, distinctCommitterName), distinctInWindow(group, i, 3, distinctCommitterEmail)), 1, 3), // distinct committers 3 deep
        normalize(Math.min(distinctInWindow(group, i, 15, distinctCommitterName), distinctInWindow(group, i, 15, distinctCommitterEmail)), 1, 15), // distinct committers 15 deep
        normalize(Math.min(distinctInWindow(group, i, 100, distinctCommitterName), distinctInWindow(group, i, 100, distinctCommitterEmail)), 1, 100), // distinct committers 100 deep
        normalize(parseFloat(row.author_date), stats[0].author_date_min, stats[0].author_date_max),
        normalize(parseFloat(row.committer_date), stats[0].committer_date_min, stats[0].committer_date_max),
        normalize(parseFloat(row.additions), stats[0].additions_min, stats[0].additions_max),
        normalize(parseFloat(row.deletions), stats[0].deletions_min, stats[0].deletions_max),
        normalize(parseFloat(row.test_additions), stats[0].test_additions_min, stats[0].test_additions_max),
        normalize(parseFloat(row.test_deletions), stats[0].test_deletions_min, stats[0].test_deletions_max)
      ]).concat(new Array(Math.max(0, 100 - group.length)).fill(new Array(12).fill(0.0))))]));
    callback(null, Object.keys(targetMap).map(key => [featureMap[key] || new Array(120).fill(0.0), targetMap[key]]));
  } catch (e) {
    console.error(e);
    callback(e);
  }
}