const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");

const PROTO_PATH = path.resolve(__dirname, "raft/mapreduce.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const grpcObj = grpc.loadPackageDefinition(packageDefinition);

function yourServiceImplementation(call, callback) {
  console.log("Received gRPC request:", call.request);
  // ...
  callback(null, {});
}

const runReducer = (numPorts) => {
  for (let i = 0; i < numPorts; i++) {
    const port = `500${i + 1}`;
    const server = new grpc.Server();
    server.addService(grpcObj.MapReduceService.service, {
      yourServiceMethod: yourServiceImplementation
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
