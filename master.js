const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");
const fs = require("fs");
const readline = require("readline");

const PROTO_PATH = path.resolve(__dirname, "raft/mapreduce.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const grpcObj = grpc.loadPackageDefinition(packageDefinition);

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
let centroids = [];

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

async function sendDataToMappers(mappersClientInfo, r) {
  let mapperResponses = {};
  await Promise.all(
    Object.entries(mappersClientInfo).map(([port, conn], i) => {
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
        const request = {
          filePath: `Data/Input_Chunks/chunk${i + 1}.txt`,
          centroids: cent,
          numReducer: r,
          key:i
        };
        conn.MasterMapper(request, (error, response) => {
          if (error) {
            console.error(`Error sending data to mapper:`, error);
            reject(error);
          } else {
            console.log(`Response from mapper:`, response.status);
            mapperResponses[port] = response;
            resolve();
          }
        });
      });
    })
  );
  return mapperResponses;
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
          (async () => {
            const mappersClientInfo = await setMappersClientInfo(m);
            setTimeout(async () => {
              await sendDataToMappers(mappersClientInfo, r);
            }, [2000]);
          })();
        });
      });
    });
  });
}

promptUser();