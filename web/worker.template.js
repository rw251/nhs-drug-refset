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
  const numberOfActiveConcepts = refSet.activeConcepts.length;
  const numberOfInactiveConcepts = refSet.inactiveConcepts.length;
  const numberOfConcepts =
    refSet.activeConcepts.length + refSet.inactiveConcepts.length;
  console.log(numberOfConcepts);
  const concepts = refSet.activeConcepts.slice(
    0,
    10000 - refSet.inactiveConcepts.length
  );
  const numberOfConceptsReturned =
    concepts.length + refSet.inactiveConcepts.length;
  const refSetHTML =
    concepts
      .map(
        (x) =>
          `<tr><td>${x}</td><td>${
            definitions[x] ? definitions[x].term : ''
          }</td></tr>`
      )
      .join('') +
    refSet.inactiveConcepts
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
      numberOfActiveConcepts,
      numberOfInactiveConcepts,
      numberOfConcepts,
      numberOfConceptsReturned,
      refSetHTML,
      refSetId,
    },
  });

  const dataActive = refSet.activeConcepts
    .map((x) => `${x}\t${definitions[x] ? definitions[x].term : ''}`)
    .join('\n');
  const dataInactive = refSet.inactiveConcepts
    .map((x) => `${x}\t${definitions[x] ? definitions[x].term : ''}`)
    .join('\n');
  postMessage({ msg: 'data', content: { dataActive, dataInactive, refSetId } });
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
    .map((x) => {
      let [part1, part2, thing] = x.split(' - ');
      if (!part2) thing = part1;
      if (!thing) thing = part2;
      thing = thing.replace(
        ' simple reference set (foundation metadata concept)',
        ''
      );
      thing = thing.replace('Quality and Outcomes Framework', 'QOF');
      thing = thing[0].toUpperCase() + thing.slice(1);
      return `<li data-id="${x}" data-part1="${part1}" data-part2="${part2}">${thing}<br><span>(${
        refSets[x].activeConcepts.length
      } active code${refSets[x].activeConcepts.length !== 1 ? 's' : ''}${
        refSets[x].inactiveConcepts.length > 0
          ? `, ${refSets[x].inactiveConcepts.length} inactive code${
              refSets[x].inactiveConcepts.length !== 1 ? 's' : ''
            }`
          : ''
      })</span></li>`;
    })

    .join('');
  console.log(new Date().toISOString(), 'Refs loaded');
  postMessage({ msg: 'refsLoaded', content: { refSetHTML } });
}
