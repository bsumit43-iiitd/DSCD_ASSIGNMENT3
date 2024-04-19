const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");
const fs = require("fs");

const PROTO_PATH = path.resolve(__dirname, "raft/mapreduce.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const grpcObj = grpc.loadPackageDefinition(packageDefinition);

let mappersClientInfo = {};

function shuffleAndSort(combined_data) {
  // Read the intermediate key-value pairs from all partition files and combine them

  // Sort the combined data by key
  combined_data.sort((a, b) => a.key - b.key);

  // Group the values by key
  const grouped_data = {};
  combined_data.forEach((pair) => {
    if (!grouped_data[pair.key]) {
      grouped_data[pair.key] = [];
    }
    grouped_data[pair.key].push({ x: pair.x, y: pair.y });
  });

  return grouped_data;
}

function reduce(data) {
  const grouped_data = shuffleAndSort(data);
  const updated_centroids = {};
  Object.entries(grouped_data).forEach(([key, values]) => {
    const average = values.reduce(
      (acc, val) => {
        return {
          x: acc.x + val.x,
          y: acc.y + val.y
        };
      },
      { x: 0, y: 0 }
    );
    average.x /= values.length;
    average.y /= values.length;
    updated_centroids[key] = average;
  });

  Object.values(updated_centroids).forEach((item) => {
    const output_file = path.join("Data/UpdatedCentroids.txt");
    fs.writeFile(output_file, `${item.x},${item.y}\n`, { flag: "a" }, (err) => {
      if (err) {
        // console.error("Error writing to file:", err);
        return;
      }
    });
  });
  return updated_centroids;
}

async function setMappersClientInfo(m) {
  return new Promise((resolve, reject) => {
    try {
      for (let i = 0; i < m; i++) {
        const client = new grpcObj.MapReduceService(
          `localhost:300${i + 1}`,
          grpc.credentials.createInsecure()
        );
        mappersClientInfo[`300${i + 1}`] = client;
      }
      resolve(mappersClientInfo);
    } catch (err) {
      console.log("error");
      reject();
    }
  });
}

function MapperReducer(conn, key, port, mapperResponses, concatData) {
  return new Promise((resolve, reject) => {
    const request = {
      key: key,
      mapperKey: parseInt(port.substring(3))
    };
    try {
      conn.MapperReducer(request, (error, response) => {
        if (error) {
          console.error(`Error mapping data to reducer:`, error);
          mapperResponses[port] = false;
          reject(error);
        } else {
          console.log(`Response from mapper:`, response.status);
          mapperResponses[port] = response.status;
          concatData.data = [...concatData.data, ...(response?.data || [])];
          resolve();
        }
      });
    } catch (error) {
      console.error(`Error sending data to reducer:`, error);
      mapperResponses[port] = false;
      reject(error);
    }
  });
}

async function requestDataToMappers(mappersClientInfo, key) {
  let mapperResponses = {};
  let concatData = { data: [] };
  await Promise.all(
    Object.entries(mappersClientInfo).map(([port, conn], i) => {
      return MapperReducer(conn, key, port, mapperResponses, concatData);
    })
  );
  let cent = reduce(concatData.data);
  return cent;
}
async function invokeReducer(call, callback) {
  fs.unlink("Data/UpdatedCentroids.txt", (err) => {
    if (err) {
      // console.error("Error deleting file:", err);
      return;
    }
    console.log("File deleted successfully.");
  });
  const { key, numMapper } = call.request;
  // ...

  await setMappersClientInfo(numMapper);
  setTimeout(async () => {
    const result = await requestDataToMappers(mappersClientInfo, key);
    callback(null, { status: true, centroids: Object.values(result) });
  }, [1000]);
}

const runReducer = (numPorts) => {
  for (let i = 0; i < numPorts; i++) {
    const port = `500${i + 1}`;
    const server = new grpc.Server();
    server.addService(grpcObj.MapReduceService.service, {
      invokeReducer: invokeReducer
    });
    server.bindAsync(
      `0.0.0.0:${port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) {
          console.error("Failed to bind reducer gRPC server:", err);
        } else {
          console.log(`Reducer gRPC server running on port ${port}`);
        }
      }
    );
  }
};

module.exports = {
  runReducer: runReducer
};
