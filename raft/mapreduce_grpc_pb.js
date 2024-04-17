// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('grpc');
var raft_mapreduce_pb = require('../raft/mapreduce_pb.js');

function serialize_MasterMapperRequest(arg) {
  if (!(arg instanceof raft_mapreduce_pb.MasterMapperRequest)) {
    throw new Error('Expected argument of type MasterMapperRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_MasterMapperRequest(buffer_arg) {
  return raft_mapreduce_pb.MasterMapperRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_MasterMapperResponse(arg) {
  if (!(arg instanceof raft_mapreduce_pb.MasterMapperResponse)) {
    throw new Error('Expected argument of type MasterMapperResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_MasterMapperResponse(buffer_arg) {
  return raft_mapreduce_pb.MasterMapperResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// Define Raft service
var MapReduceServiceService = exports.MapReduceServiceService = {
  masterMapper: {
    path: '/MapReduceService/MasterMapper',
    requestStream: false,
    responseStream: false,
    requestType: raft_mapreduce_pb.MasterMapperRequest,
    responseType: raft_mapreduce_pb.MasterMapperResponse,
    requestSerialize: serialize_MasterMapperRequest,
    requestDeserialize: deserialize_MasterMapperRequest,
    responseSerialize: serialize_MasterMapperResponse,
    responseDeserialize: deserialize_MasterMapperResponse,
  },
};

exports.MapReduceServiceClient = grpc.makeGenericClientConstructor(MapReduceServiceService);
