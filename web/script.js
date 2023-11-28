const $versionPicker = document.getElementById('version');
const $versionWrapper = document.getElementById('version-wrapper');
const $wrapper = document.querySelector('.wrapper');
const $inp = document.querySelector('#inp');
const $listGroup = document.querySelector('.list .list-group');
const $refSetWrapper = document.getElementById('ref-set-wrapper');
const $refSetCodesPanel = document.getElementById('ref-set-codes');
const $refSetCodes = document.querySelector('#ref-set-codes tbody');
const $title = document.querySelector('#ref-set-codes h3');
const $header = document.querySelector('.header');
const $message = document.getElementById('message');
const $copy = document.querySelector('button');

const worker = new Worker('/web/worker.js');
const loader =
  '<div class="lds-facebook"><div></div><div></div><div></div></div>';

let data;

worker.onmessage = (e) => {
  const { msg, content } = e.data;
  console.log('Message received from worker', msg);
  switch (msg) {
    case 'defsLoaded':
      console.log('definitions loaded');
      break;
    case 'refsLoaded':
      console.log('refs loaded');
      $listGroup.innerHTML = content.refSetHTML;
      $list = $listGroup.querySelectorAll('li');
      $wrapper.style.display = 'grid';
      document.querySelector('.scrollable-refs').style.height = `${
        window.innerHeight - elHeight($inp) - 2 //border of ul wrapper
      }px`;
      $versionWrapper.style.display = 'none';
      $inp.focus();
      break;
    case 'data': {
      data = content.data;
      $copy.removeAttribute('disabled', '');
      break;
    }
    case 'refset':
      if (content.numberOfConcepts === content.numberOfConceptsReturned) {
        $message.innerText = `${content.numberOfConcepts} codes`;
      } else {
        $message.innerText = `${content.numberOfConceptsReturned} (out of ${content.numberOfConcepts}) codes displayed.`;
      }
      $refSetCodes.innerHTML = content.refSetHTML;
      $title.innerText = content.refSetId;
      break;
  }
};

let definitions;
let refSets;
let list;

async function setupRoutes() {
  const routes = await fetch('/web/routes.json').then((x) => x.json());
  $versionPicker.innerHTML = `<option disabled selected>Please select SNOMED version</option>${routes.map(
    (x) => `<option>${x}</option>`
  )}`;
}

$versionPicker.addEventListener('change', async (event) => {
  const folder = event.target.value;
  worker.postMessage({ action: 'load', params: { folder } });
  $versionWrapper.innerHTML = `<div style="padding-top:10px">Loading the data for ${folder}</div>${loader}`;
});

setupRoutes();

function filter_list() {
  let re = new RegExp($inp.value, 'i');
  $list.forEach((x) => {
    if (re.test(x.textContent)) {
      x.innerHTML = x.textContent.replace(re, '<b>$&</b>');
      x.style.display = 'block';
    } else {
      x.style.display = 'none';
    }
  });
}

function elHeight(el) {
  const styles = window.getComputedStyle(el);
  return (
    el.offsetHeight +
    parseFloat(styles['margin-top']) +
    parseFloat(styles['margin-bottom'])
  );
}

$listGroup.addEventListener('click', (e) => {
  $list.forEach((x) => x.classList.remove('selected'));
  const refSetId = e.target.dataset.id;
  e.target.classList.add('selected');

  $refSetCodesPanel.style.display = 'block';

  $title.innerText = refSetId;
  $refSetCodes.innerHTML = loader;

  document.querySelector('.scrollable-defs').style.height = `${
    window.innerHeight - elHeight($header)
  }px`;

  $copy.setAttribute('disabled', '');

  worker.postMessage({ action: 'defs', params: { refSetId } });
});

$copy.addEventListener('click', async (e) => {
  const now = new Date();
  $copy.setAttribute('disabled', '');
  $copy.innerText = 'Copying...';
  // Copy the text inside the text field
  await navigator.clipboard.writeText(data);

  const diff = new Date() - now;
  setTimeout(() => {
    $copy.removeAttribute('disabled', '');
    $copy.innerText = 'Copied!';
    setTimeout(() => {
      $copy.innerText = 'Copy';
    }, 2000);
  }, Math.max(0, 500 - diff));
});
