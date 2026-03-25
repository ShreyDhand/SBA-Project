/* UPDATE THESE VALUES TO MATCH YOUR SETUP */

const PROCESSING_STATS_API_URL = "http://localhost:8100/stats";

const ANALYZER_API_URL = {
    stats: "http://localhost:8110/stats",
    shot: "http://localhost:8110/hockey/shots?index=0",
    penalty: "http://localhost:8110/hockey/penalties?index=0"
};

// NEW: Health endpoint
const HEALTH_URL = "http://localhost:8120/health/status";

// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result);
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message);
        });
};

const updateCodeDiv = (result, elemId) => {
    const elem = document.getElementById(elemId);
    if (elem) {
        elem.innerText = JSON.stringify(result, null, 2);
    }
};

const getLocaleDateStr = () => (new Date()).toLocaleString();

// NEW: Update health status
function updateHealthStatus() {
    fetch(HEALTH_URL)
        .then(response => response.json())
        .then(data => {
            document.getElementById("status-receiver").innerText = data.receiver;
            document.getElementById("status-storage").innerText = data.storage;
            document.getElementById("status-processing").innerText = data.processing;
            document.getElementById("status-analyzer").innerText = data.analyzer;
            document.getElementById("status-last-update").innerText = data.last_update;
        })
        .catch(error => {
            console.error("Error fetching health status:", error);
            updateErrorMessages(error.message);
        });
}

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr();

    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"));
    makeReq(ANALYZER_API_URL.stats, (result) => updateCodeDiv(result, "analyzer-stats"));
    makeReq(ANALYZER_API_URL.shot, (result) => updateCodeDiv(result, "event-shot"));
    makeReq(ANALYZER_API_URL.penalty, (result) => updateCodeDiv(result, "event-penalty"));
};

const updateErrorMessages = (message) => {
    const id = Date.now();
    const msg = document.createElement("div");
    msg.id = `error-${id}`;
    msg.innerHTML = `<p style="color:red;">Connection error at ${getLocaleDateStr()}!</p><code>${message}</code>`;
    document.getElementById("messages").style.display = "block";
    document.getElementById("messages").prepend(msg);

    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`);
        if (elem) { elem.remove(); }
    }, 3500);
};

const setup = () => {
    getStats();
    updateHealthStatus(); // NEW: run once on load

    setInterval(() => getStats(), 4000);     // existing stats refresh
    setInterval(() => updateHealthStatus(), 5000); // NEW: health refresh
};

document.addEventListener('DOMContentLoaded', setup);