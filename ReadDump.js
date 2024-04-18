const fs = require("fs");
const path = require("path");
const readline = require("readline");

const logToFile = (level, message, filename) => {
    const timestamp = new Date().toISOString();
  
    const logMessage = `[${timestamp}] [${level.toUpperCase()}] - ${message}\n`;
  
    const logFilePath = path.join(__dirname, `logs`, filename);
  
    fs.appendFile(logFilePath, logMessage, { flag: "a+" }, (err) => {
      if (err) {
        console.error(`Error writing to log file ${filename}:`, err);
      }
    });
  };

function CreateDumpFile() {
    return new Promise((resolve, reject) => {
        const logDir = path.join(__dirname, `logs`);
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir);
        }

        ["dump.txt"].forEach((filename) => {
            const logFilePath = path.join(logDir, filename);
            if (!fs.existsSync(logFilePath)) {
                fs.writeFileSync(logFilePath, "");
                resolve(); // Resolve the promise once file creation is complete
            }
        });
    });
}

module.exports = {logToFile, CreateDumpFile};
