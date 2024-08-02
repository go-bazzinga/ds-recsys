from concurrent import futures
import contextlib
import datetime
import logging
import multiprocessing
import socket
import sys
import time
import os
from typing import Any
import jwt
import consts

import grpc
from grpc_reflection.v1alpha import reflection
from recommendation_service import video_recommendation_pb2
from recommendation_service import video_recommendation_pb2_grpc

from utils.upstash_utils import UpstashUtils
from simple_recommendation_v0 import SimpleRecommendationV0

_LOGGER = logging.getLogger(__name__)

_ONE_DAY = datetime.timedelta(days=1)
_PROCESS_COUNT = multiprocessing.cpu_count()
_THREAD_CONCURRENCY = 10 # heuristic
_BIND_ADDRESS = "[::]:50051"  # Fixed bind address


_AUTH_HEADER_KEY = "authorization"

_PUBLIC_KEY = consts.RECSYS_JWT_PUB_KEY
_JWT_PAYLOAD = {
    "sub": "yral-recsys-server",
    "company": "gobazzinga",
}


class SignatureValidationInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        def abort(ignored_request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid signature")

        self._abort_handler = grpc.unary_unary_rpc_method_handler(abort)

    def intercept_service(self, continuation, handler_call_details):
        metadata_dict = dict(handler_call_details.invocation_metadata)
        token = metadata_dict[_AUTH_HEADER_KEY].split()[1]
        payload = jwt.decode(
            token,
            _PUBLIC_KEY,
            algorithms=["EdDSA"],
        )

        if payload == _JWT_PAYLOAD:
            return continuation(handler_call_details)
        else:
            print(f"Received payload: {payload}")
            return self._abort_handler


class VideoRecommendationServicer(video_recommendation_pb2_grpc.VideoRecommendationServicer):
    def __init__(self):
        self.recommender = SimpleRecommendationV0()

    def GetRecommendations(self, request, context):
        userId = request.userId
        watchHistory = request.watchHistory
        successfulPlays = request.successfulPlays

        recommended_videos = self.recommender.get_recommendation(successfulPlays, watchHistory)
        return video_recommendation_pb2.RecommendationResponse(videoIds=recommended_videos)

def _wait_forever(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        server.stop(None)

def _run_server():
    _LOGGER.info("Starting new server.")
    options = (("grpc.so_reuseport", 1),)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=_THREAD_CONCURRENCY),
        interceptors=(SignatureValidationInterceptor(),),
        options=options,
    )
    video_recommendation_pb2_grpc.add_VideoRecommendationServicer_to_server(
        VideoRecommendationServicer(), server
    )
    SERVICE_NAMES = (
        video_recommendation_pb2.DESCRIPTOR.services_by_name['VideoRecommendation'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port(_BIND_ADDRESS)
    server.start()
    _LOGGER.info(f"Server started on {_BIND_ADDRESS}")
    _wait_forever(server)

def main():
    multiprocessing.set_start_method("spawn", force=True)
    _LOGGER.info(f"Binding to '{_BIND_ADDRESS}'")
    sys.stdout.flush()
    
    
    workers = []
    for _ in range(_PROCESS_COUNT):
        worker = multiprocessing.Process(target=_run_server)
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()

if __name__ == "__main__":
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[PID %(process)d] %(message)s")
    handler.setFormatter(formatter)
    _LOGGER.addHandler(handler)
    _LOGGER.setLevel(logging.INFO)
    main()