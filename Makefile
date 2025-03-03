ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

proto: proto/data_nexus.proto
	protoc -I=$(ROOT_DIR)/proto \
				 --go_out=$(ROOT_DIR)/proto \
				 --go_opt=module=github.com/haze518/data-nexus/proto \
				 --go-grpc_out=$(ROOT_DIR)/proto \
        		 --go-grpc_opt=module=github.com/haze518/data-nexus/proto \
				 $(ROOT_DIR)/proto/data_nexus.proto