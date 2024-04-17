const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");
const fs = require("fs");

const PROTO_PATH = path.resolve(__dirname, "raft/mapreduce.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const grpcObj = grpc.loadPackageDefinition(packageDefinition);

function euclideanDistance(point1, point2) {
  // Calculate Euclidean distance between two points
  let sum = 0;
  for (let i = 0; i < point1.length; i++) {
    sum += Math.pow(point1[i] - point2[i], 2);
  }
  return Math.sqrt(sum);
}

function findNearestCentroid(centroids, data_point) {
  // Find the nearest centroid for the given data point
  let min_distance = Number.MAX_VALUE;
  let nearest_centroid_index = -1;

  centroids.forEach((centroid, index) => {
    const distance = euclideanDistance(centroid, data_point);
    if (distance < min_distance) {
      min_distance = distance;
      nearest_centroid_index = index;
    }
  });

  return { index: nearest_centroid_index, distance: min_distance };
}

function map(centroids, input_file, num_reducers, key) {
  return new Promise((resolve, reject) => {
    try {
      // Read the input split assigned by the master
      const data = fs.readFileSync(input_file, "utf8").split("\n");

      // For each data point, find the nearest centroid and emit key-value pair
      const mapped_data = data.map((point) => {
        const nearest_centroid = findNearestCentroid(
          centroids,
          point.split(",").map(Number)
        );
        return { key: nearest_centroid.index, value: point };
      });

      // Partition the mapped data
      const partitions = {};
      mapped_data.forEach((pair) => {
        const reducer_index = pair.key % num_reducers;
        if (!partitions[reducer_index]) {
          partitions[reducer_index] = [];
        }
        partitions[reducer_index].push(pair);
      });

      if (!fs.existsSync("Data/Mappers")) {
        fs.mkdirSync("Data/Mappers");
      }
      // Create folder if not exists
      const folderPath = path.join("Data/Mappers", `M${key}`);
      if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath);
      }

      // Write partitioned data to partition files
      Object.keys(partitions).forEach((reducer_index) => {
        const partition_file = path.join(
          folderPath,
          `Partition_${reducer_index}.txt`
        );
        fs.writeFileSync(
          partition_file,
          JSON.stringify(partitions[reducer_index])
        );
      });

      // Introduce probabilistic failure
      const probabilisticFlag = Math.random() < 0.5; // 50% probability of failure
      // const probabilisticFlag = 1;
      if (probabilisticFlag) {
        console.log("Mapping successful.");
        resolve(
          Object.values(partitions).map((_, index) =>
            path.join(folderPath, `Partition_${index}.txt`)
          )
        );
      } else {
        console.log("Mapping unsuccessful or failed.");
        reject("Mapping unsuccessful or failed."); // Indicate failure by rejecting with an error message
      }
    } catch (error) {
      console.error("Error:", error);
      reject(error); // Reject with the error encountered during execution
    }
  });
}

function yourServiceImplementation(call, callback) {
  console.log("Received gRPC request:", call.request);
  const { filePath, centroids, numReducer, key } = call.request;
  const centroidCoordinates = centroids.map((centroid) => [
    centroid.x,
    centroid.y
  ]);
  map(centroidCoordinates, filePath, numReducer, key)
    .then((partitionFiles) => {
      callback(null, {
        status: true
      });
      console.log("Partition files:", partitionFiles);
    })
    .catch((error) => {
      callback(null, {
        status: false
      });
      console.error("Error:", error);
    });
}

const runMapper = (numPorts) => {
  for (let i = 0; i < numPorts; i++) {
    const port = `300${i + 1}`;
    const server = new grpc.Server();
    server.addService(grpcObj.MapReduceService.service, {
      MasterMapper: yourServiceImplementation
    });
    server.bindAsync(
      `0.0.0.0:${port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) {
          console.error("Failed to bind mapper gRPC server:", err);
        } else {
          console.log(`Mapper gRPC server running on port ${port}`);
        }
      }
    );
  }
};

module.exports = {
  runMapper: runMapper
};
