// Quick unit tests for the blame parsing + reviewer selection logic
// This file is meant to be run with `node .github/workflows/tests/request-review-test.js`

function latestBlameCommit(blameOutput) {
  let latest = null;
  let current = null;

  function finalizeCurrent() {
    if (!current || current.authorTime == null) return;
    if (!latest || current.authorTime > latest.authorTime) {
      latest = current;
    }
  }

  for (const line of blameOutput.split(/\r?\n/)) {
    const header = line.match(/^([0-9a-f^]+)\s+\d+\s+\d+\s+\d+$/);
    if (header) {
      finalizeCurrent();
      current = { sha: header[1].replace(/^\^/, ''), authorTime: null };
      continue;
    }
    const authorTime = line.match(/^author-time\s+(\d+)$/);
    if (authorTime && current) current.authorTime = Number(authorTime[1]);
  }

  finalizeCurrent();
  return latest;
}

// Simple test for latestBlameCommit
const sampleBlame = [
  'aaaaaaaa 1 1 1',
  'author Alice',
  'author-time 100',
  '',
  'bbbbbbbb 2 2 2',
  'author Bob',
  'author-time 200',
  '',
].join('\n');

const latest = latestBlameCommit(sampleBlame);
console.log('latest sha:', latest && latest.sha);
console.log('expected sha: bbbbbbbb');

// Now exercise the higher-level flow with mocked execFileSync and getCommit
async function getReviewersFromBlameMock({ files, pullBaseSha, author }) {
  // Map filenames to blame outputs with different SHAs
  const blameMap = {
    'a.txt': ['11111111 1 1 1', 'author X', 'author-time 150', ''].join('\n'),
    'b.txt': ['22222222 1 1 1', 'author Y', 'author-time 250', ''].join('\n'),
    'c.txt': ['33333333 1 1 1', 'author Z', 'author-time 50', ''].join('\n'),
  };

  function execFileSyncMock(cmd, args, opts) {
    const file = args[args.length - 1];
    if (!blameMap[file]) throw new Error('file not found: ' + file);
    return blameMap[file];
  }

  async function getCommitMock({ ref }) {
    // map refs to fake commit objects with author login
    const map = {
      '11111111': { author: { login: 'alice', type: 'User' } },
      '22222222': { author: { login: 'bob', type: 'User' } },
      '33333333': { author: { login: 'carol', type: 'User' } },
    };
    if (!map[ref]) throw new Error('commit not found: ' + ref);
    return { data: map[ref] };
  }

  // Inline copy of the logic tuned for mocks
  function latestCommitForBlameOutput(blameOutput) {
    return latestBlameCommit(blameOutput);
  }

  const reviewers = new Set();
  for (const filename of files) {
    let blameOutput;
    try {
      blameOutput = execFileSyncMock('git', ['blame', '-p', pullBaseSha, '--', filename], { encoding: 'utf8' });
    } catch (e) {
      console.warn('blame failed', e.message);
      continue;
    }
    const latest = latestCommitForBlameOutput(blameOutput);
    if (!latest) continue;
    let commit;
    try {
      ({ data: commit } = await getCommitMock({ ref: latest.sha }));
    } catch (e) {
      console.warn('commit lookup failed', e.message);
      continue;
    }
    const login = commit.author?.login ?? commit.committer?.login;
    if (!login) continue;
    if (login.toLowerCase() === (author || '').toLowerCase()) continue;
    reviewers.add(login);
  }

  return [...reviewers].sort();
}

(async () => {
  const result = await getReviewersFromBlameMock({ files: ['a.txt', 'b.txt', 'c.txt'], pullBaseSha: 'base', author: 'david' });
  console.log('mock reviewers:', result);
  console.log('expected reviewers: ["alice","bob","carol"]');
})();
