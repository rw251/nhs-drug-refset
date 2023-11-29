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
const $copy = document.getElementById('copy');
const $tab = document.querySelector('.tab');

const worker = new Worker('/web/worker.js');
const loader =
  '<div class="lds-facebook"><div></div><div></div><div></div></div>';

let data;
let refSetTimer;

worker.onmessage = (e) => {
  const { msg, content } = e.data;
  console.log('Message received from worker', msg);
  switch (msg) {
    case 'defsLoaded':
      console.log('definitions loaded');
      break;
    case 'refsLoaded':
      clearTimeout(refSetTimer);
      console.log('refs loaded');
      $listGroup.innerHTML = content.refSetHTML;
      $list = $listGroup.querySelectorAll('li');
      $wrapper.style.display = 'grid';
      document.querySelector('.scrollable-refs').style.height = `${
        window.innerHeight - elHeight($inp) - elHeight($tab) - 38 //border of ul wrapper (2) + banner (36)
      }px`;
      $versionWrapper.style.display = 'none';
      filter_list();
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
  $versionPicker.innerHTML = `<option disabled selected>Please select SNOMED version</option>${routes
    .sort((b, a) => {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    })
    .map((x) => {
      const [region, description, version, date] = x.split('_');
      let readableVersion = x;
      if (
        region === 'uk' &&
        description.indexOf('sct') === 0 &&
        version.match(/^[0-9]+\.[0-9]+\.[0-9]+$/) &&
        date.match(/^[0-9]{14}Z$/)
      ) {
        readableVersion = `v${version} (${date.substring(
          0,
          4
        )}/${date.substring(4, 6)}/${date.substring(6, 8)})`;
      }
      return `<option value="${x}">${readableVersion}</option>`;
    })}`;
}

$versionPicker.addEventListener('change', async (event) => {
  $wrapper.style.display = 'none';
  $refSetCodesPanel.style.display = 'none';
  const folder = event.target.value;
  worker.postMessage({ action: 'load', params: { folder } });
  refSetTimer = setTimeout(() => {
    $versionWrapper.innerHTML = `<div style="padding-top:10px">Loading the data for ${folder}</div>${loader}`;
    $versionWrapper.style.display = 'block';
  }, 100);
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

  openTab('Codes');
  $refSetCodesPanel.style.display = 'block';

  $title.innerText = refSetId;
  $refSetCodes.innerHTML = loader;

  document.querySelector('.scrollable-defs').style.height = `${
    window.innerHeight - elHeight($header) - elHeight($tab) - 36 // 36 = header
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

function openTab(tabName) {
  if (window.getComputedStyle($tab).display === 'none') return;

  $refSetWrapper.style.display = 'none';
  $refSetCodesPanel.style.display = 'none';

  const tablinks = document.getElementsByClassName('tablinks');

  if (tabName === 'Codes') {
    tablinks[0].classList.remove('active');
    tablinks[1].classList.add('active');
    $refSetCodesPanel.style.display = 'block';
  }
  if (tabName === 'Refsets') {
    tablinks[0].classList.add('active');
    tablinks[1].classList.remove('active');
    $refSetWrapper.style.display = 'block';
  }
}
