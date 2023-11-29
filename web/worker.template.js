/*

Get definitions
*/

let definitions;
let refSets;
let refSetNames;

function getDefs(refSetId) {
  if (!definitions) {
    console.log('Definitions not loaded... Waiting 500ms...');
    return setTimeout(() => {
      getDefs(refSetId);
    }, 500);
  }
  console.log('Definitions now loaded.');
  const refSet = refSets[refSetId];
  console.log(refSet.activeConcepts.length);
  console.log(refSet.inactiveConcepts.length);
  const numberOfConcepts =
    refSet.activeConcepts.length + refSet.inactiveConcepts.length;
  console.log(numberOfConcepts);
  const activeConcepts = refSet.activeConcepts.slice(0, 9990);
  const inactiveConcepts = refSet.inactiveConcepts.slice(
    0,
    Math.max(10, 10000 - activeConcepts.length)
  );
  const numberOfConceptsReturned =
    activeConcepts.length + inactiveConcepts.length;
  const refSetHTML =
    activeConcepts
      .map(
        (x) =>
          `<tr><td>${x}</td><td>${
            definitions[x] ? definitions[x].term : ''
          }</td></tr>`
      )
      .join('') +
    inactiveConcepts
      .map(
        (x) =>
          `<tr class="inactive"><td>${x}</td><td>${
            definitions[x] ? definitions[x].term : ''
          }</td></tr>`
      )
      .join('');
  postMessage({
    msg: 'refset',
    content: {
      numberOfConcepts,
      numberOfConceptsReturned,
      refSetHTML,
      refSetId,
    },
  });

  const data =
    refSet.activeConcepts
      .map((x) => `${x}\t${definitions[x] ? definitions[x].term : ''}`)
      .join('\n') +
    '\n' +
    refSet.inactiveConcepts
      .map((x) => `${x}\t${definitions[x] ? definitions[x].term : ''}`)
      .join('\n');
  postMessage({ msg: 'data', content: { data, refSetId } });
}

onmessage = (e) => {
  const { action, params } = e.data;
  switch (action) {
    case 'load':
      const { folder } = params;
      loadRefSets(folder);
      loadDefinitions(folder);
      break;
    case 'defs':
      const { refSetId } = params;
      getDefs(refSetId);
      break;
    default:
      console.log('Incorrect action received by worker', action);
  }
};

async function loadDefinitions(folder) {
  console.log('loading defs...');
  definitions = await fetch(
    `{URL}/files/processed/${folder}/defs-0-9999.json`
  ).then((x) => x.json());
  console.log(new Date().toISOString(), 'Defs loaded');
  postMessage({ msg: 'defsLoaded' });
}

async function loadRefSets(folder) {
  console.log('loading refs...');
  refSets = await fetch(
    `{URL}/files/processed/${folder}/refSets-0-9999.json`
  ).then((x) => x.json());
  refSetNames = Object.keys(refSets);
  const refSetHTML = refSetNames
    .map(
      (x) =>
        `<li data-id="${x}">${x} (${refSets[x].activeConcepts.length} codes)</li>`
    )
    .join('');
  console.log(new Date().toISOString(), 'Refs loaded');
  postMessage({ msg: 'refsLoaded', content: { refSetHTML } });
}
