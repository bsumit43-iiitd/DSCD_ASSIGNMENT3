const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");
const fs = require("fs");
const readline = require("readline");

const PROTO_PATH = path.resolve(__dirname, "raft/mapreduce.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const grpcObj = grpc.loadPackageDefinition(packageDefinition);
const { logToFile, CreateDumpFile } = require("./ReadDump.js");
const { runMapper } = require("./mapper.js");
const { runReducer } = require("./reducer.js");

global.NUM_MAPPER;
global.NUM_REDUCER;
global.NUM_CENTROID;
global.NUM_ITERATION;

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

let mappersClientInfo = {};
let reducerClientInfo = {};
let centroids = [];

CreateDumpFile().then((res) => {
  //console.log("Dump file is created");
});

function getRandomCentroids(inputFilePath, num_centroids) {
  const data = fs.readFileSync(inputFilePath, "utf8").split("\n");
  const centroids = [];
  const num_data_points = data.length;

  while (centroids.length < num_centroids) {
    const random_index = Math.floor(Math.random() * num_data_points);
    centroids.push(data[random_index]);
  }

  return centroids;
}

function split_input(inputFilePath, numberOfMappers) {
  const data = fs.readFileSync(inputFilePath, "utf8").split("\n");
  const quotient = Math.floor(data.length / numberOfMappers);
  const remainder = data.length % numberOfMappers;
  let input_chunks = [];
  for (let i = 0, j = 0; i < data.length - remainder; i += quotient, j++) {
    input_chunks.push(data.slice(i, i + quotient));
  }
  for (let i = 0; i < remainder; i++) {
    input_chunks[i % input_chunks.length].push(
      data[quotient * numberOfMappers + i]
    );
  }
  let chunk_paths = [];
  const input_filename = inputFilePath.split("/").pop();
  const input_dir = inputFilePath.split("/").slice(0, -1).join("/");
  input_chunks.forEach(async (chunk, index) => {
    const chunk_path = `${input_dir}_Chunks/chunk${index + 1}.txt`;
    console.log(chunk_path);
    if (!fs.existsSync(`${input_dir}_Chunks`)) {
      fs.mkdirSync(`${input_dir}_Chunks`);
    }
    fs.writeFileSync(chunk_path, chunk.join("\n"));
    chunk_paths.push(chunk_path);
  });
  return chunk_paths;
}

async function setMappersClientInfo(m) {
  return new Promise((resolve, reject) => {
    for (let i = 0; i < m; i++) {
      const client = new grpcObj.MapReduceService(
        `localhost:300${i + 1}`,
        grpc.credentials.createInsecure()
      );
      mappersClientInfo[`300${i + 1}`] = client;
    }
    resolve(mappersClientInfo);
  });
}

async function setReducerClientInfo(r) {
  return new Promise((resolve, reject) => {
    for (let i = 0; i < r; i++) {
      const client = new grpcObj.MapReduceService(
        `localhost:500${i + 1}`,
        grpc.credentials.createInsecure()
      );
      reducerClientInfo[`500${i + 1}`] = client;
    }
    resolve(reducerClientInfo);
  });
}

async function sendDataToMappers(mappersClientInfo, r, filePaths, inter = 0) {
  // let mapperResponses = {};
  let mapperResponses = Object.keys(mappersClientInfo).reduce((obj, key) => {
    obj[key] = false;
    return obj;
    ("");
  }, {});
  await Promise.all(
    Object.entries(mappersClientInfo).map(([port, conn], i) => {
      logToFile("info",
             `Requesting Mapper ${port}`,
             "dump.txt");
      return new Promise((resolve, reject) => {
        cent = [];
        centroids?.map((centroid) => {
          const [xStr, yStr] = centroid.split(",");
          const x = parseFloat(xStr);
          const y = parseFloat(yStr);
          const c = {
            x: x,
            y: y
          };
          cent.push(c);
        });
        if (filePaths[i]) {
          const request = {
            filePath: filePaths[i],
            centroids: cent,
            numReducer: r,
            key: i + 1
          };
          try {
            conn.MasterMapper(request, (error, response) => {
              if (error) {
                console.error(`Error mapping data to mapper:`, error);
                mapperResponses[port] = response.status;
                logToFile("info",
                "Mapper Status: FAILURE",
                "dump.txt");
                reject(error);
              } else {
                console.log(`Response from mapper:`, response.status);
                mapperResponses[port] = response.status;
                logToFile("info",
                "Mapper Status: SUCCESS",
                "dump.txt");
                resolve();
              }
            });
          } catch (error) {
            console.error(`Error sending data to mapper:`, error);
            mapperResponses[port] = response.status;
            reject(error);
          }
        } else {
          resolve();
        }
      });
    })
  );

  const filteredMappersClientInfo = Object.entries(mapperResponses)
    .filter(([key, value]) => value === "true")
    .reduce((obj, [key]) => {
      if (mappersClientInfo.hasOwnProperty(key)) {
        obj[key] = mappersClientInfo[key];
      }
      return obj;
    }, {});

  const tempLength = Object.keys(mapperResponses).length;

  let filteredFilePaths = Object.keys(mapperResponses)
    .map((key, index) => {
      if (mapperResponses[key] === "false") return filePaths[index];
    })
    .filter((item) => item);

  filteredFilePaths = filteredFilePaths.concat(filePaths.slice(tempLength));

  if (
    filteredFilePaths?.length &&
    Object.keys(filteredMappersClientInfo)?.length &&
    inter < 4
  ) {
    return await sendDataToMappers(
      filteredMappersClientInfo,
      r,
      filteredFilePaths,
      inter++
    );
  }
  return filteredFilePaths;
}

function invokeReducer(conn, m, i, port, reducerResponses, centroids) {
  return new Promise((resolve, reject) => {
    const request = {
      numMapper: m,
      key: i + 1
    };
    try {
      conn.InvokeReducer(request, (error, response) => {
        if (error) {
          console.error(`Error mapping data to reducer:`, error);
          reducerResponses[port] = response.status;
          logToFile("info",
             "Reducer Status: FAILURE",
             "dump.txt");
          reject(error);
        } else {
          console.log(`Response from reducer:`, response.status);
          reducerResponses[port] = response.status;
          logToFile("info",
             "Reducer Status: SUCCESS",
             "dump.txt");
          resolve(response?.centroids);
        }
      });
    } catch (error) {
      console.error(`Error sending data to reducer:`, error);
      reducerResponses[port] = response.status;
      reject(error);
    }
  });
}

function centroidsConverged(oldCentroids, newCentroids, tolerance) {
  // Check if the lengths of old and new centroids arrays match
  if (oldCentroids.length !== newCentroids.length) {
    return false;
  }

  // Iterate over each centroid
  for (let i = 0; i < oldCentroids.length; i++) {
    const oldCentroid = oldCentroids[i];
    const newCentroid = newCentroids[i];

    // Check if the distance between old and new centroids is within the tolerance
    const distance = Math.sqrt(
      Math.pow(oldCentroid.x - newCentroid.x, 2) +
        Math.pow(oldCentroid.y - newCentroid.y, 2)
    );
    if (distance > tolerance) {
      return false;
    }
  }

  // If all centroids are within the tolerance, return true (convergence)
  return true;
}

function convertPointsToPointObjects(points) {
  return points.map((point) => {
    const [x, y] = point.split(",").map(parseFloat);
    return { x, y };
  });
}

async function sendRequestToReducers(reducerClientInfo, r, m, inter = 0) {
  let reducerResponses = {};
  let allCentroids = []; // Array to accumulate all centroids
  await Promise.all(
    Object.entries(reducerClientInfo).map(([port, conn], i) => {
      logToFile("info",
             `Requesting Reducer ${port}`,
             "dump.txt");
      return invokeReducer(conn, m, i, port, reducerResponses)
        .then((centroids) => {
          if (centroids) {
            allCentroids = allCentroids.concat(centroids); // Concatenate centroids
          }
        })
        .catch((error) => console.error(`Error from reducer ${port}:`, error));
    })
  );
  return allCentroids;
}

function sendDataAndReceiveNewCentroids(m, r, filePaths) {
  return new Promise((resolve, reject) => {
    setMappersClientInfo(m)
      .then(async (mappersClientInfo) => {
        setTimeout(async () => {
          try {
            const result = await sendDataToMappers(
              mappersClientInfo,
              r,
              filePaths
            );
            console.log("result:", result);
            if (result?.length) {
              console.log(
                "Mapper is down. Not able to map all the data points/chunks"
              );
            } else {
              const reducerClientInfo = await setReducerClientInfo(r);
              setTimeout(async () => {
                try {
                  const newCentroids = await sendRequestToReducers(
                    reducerClientInfo,
                    r,
                    m
                  );
                  resolve(newCentroids);
                } catch (error) {
                  console.error("Error sending request to reducers:", error);
                  reject(error);
                }
              }, 2000);
            }
          } catch (error) {
            console.error("Error sending data to mappers:", error);
            reject(error);
          }
        }, 2000);
      })
      .catch((error) => {
        console.error("Error setting mappers client info:", error);
        reject(error);
      });
  });
}

async function runIterations(n, m, r, filePaths) {
  let newCentroids = [];
  for (let i = 0; i < n; i++) {
    try {
      logToFile("info",
        `Iteration Number: ${i}`,
        "dump.txt");

      logToFile("info",
        `Centroid: ${centroids}`,
        "dump.txt");
      newCentroids = await sendDataAndReceiveNewCentroids(m, r, filePaths);

      const centroidsFormatted = newCentroids.map(({ x, y }) => `${x},${y}`);
      newCentroids = centroidsFormatted;

      const tolerance = 0.1;

      // Check if centroids have converged
      const converged = centroidsConverged(
        convertPointsToPointObjects(centroids),
        convertPointsToPointObjects(newCentroids),
        tolerance
      );
      console.log("Concergence is ", converged)
      console.log(`Iteration ${i + 1}: New centroids received.`);
      if (converged) break;
      
      centroids = newCentroids;
      // Check for convergence
    } catch (error) {
      console.error(`Iteration ${i + 1}: Error occurred:`, error);
      break; // Break the loop on error
    }
  }
  logToFile("info",
        `Final Centroid: ${newCentroids}`,
        "dump.txt");
  return newCentroids;
}

function promptUser() {
  rl.question("Enter Number of Mapper : ", (m) => {
    rl.question("Enter Number of Reducer : ", (r) => {
      rl.question("Enter Number of Centroids : ", (k) => {
        rl.question("Enter Number of Iterations : ", (n) => {
          NUM_MAPPER = m;
          NUM_REDUCER = r;
          NUM_CENTROID = k;
          NUM_ITERATION = n;
          runMapper(m);
          runReducer(r);
          centroids = getRandomCentroids("Data/Input/points.txt", k);
          split_input("Data/Input/points.txt", m);
          let filePaths = [];
          for (let a = 0; a < m; a++) {
            filePaths.push(`Data/Input_Chunks/chunk${a + 1}.txt`);
          }
          runIterations(n, m, r, filePaths)
            .then((finalCentroids) => {
              console.log("Final centroids:", finalCentroids);
            })
            .catch((error) => {
              console.error("Error:", error);
            });
        });
      });
    });
  });
}

promptUser();
