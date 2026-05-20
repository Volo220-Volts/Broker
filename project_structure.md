# 📁 broker - Project Structure

*Generated on: 5/19/2026, 5:57:49 PM*

## 📋 Quick Overview

| Metric | Value |
|--------|-------|
| 📄 Total Files | 53 |
| 📁 Total Folders | 26 |
| 🌳 Max Depth | 8 levels |

## ⭐ Important Files

- 🟡 🚫 **.gitignore** - Git ignore rules

## 📊 File Statistics

### By File Type

- 📄 **.java** (Other files): 39 files (73.6%)
- 📄 **.** (Other files): 3 files (5.7%)
- 📖 **.md** (Markdown files): 3 files (5.7%)
- 🚫 **.gitignore** (Git ignore): 1 files (1.9%)
- 📄 **.properties** (Other files): 1 files (1.9%)
- 📄 **.db** (Other files): 1 files (1.9%)
- 📄 **.txt** (Text files): 1 files (1.9%)
- 📄 **.cmd** (Other files): 1 files (1.9%)
- ⚙️ **.xml** (XML files): 1 files (1.9%)
- 📄 **.proto** (Other files): 1 files (1.9%)
- ⚙️ **.yaml** (YAML files): 1 files (1.9%)

### By Category

- **Other**: 46 files (86.8%)
- **Docs**: 4 files (7.5%)
- **Config**: 2 files (3.8%)
- **DevOps**: 1 files (1.9%)

### 📁 Largest Directories

- **root**: 53 files
- **src**: 42 files
- **src/main**: 35 files
- **src/main/java/com/volo/broker**: 32 files
- **src/main/java/com/volo**: 32 files

## 🌳 Directory Structure

```
broker/
├── 📁 src/
│   ├── 📂 main/
│   │   ├── 📂 java/
│   │   │   └── 📂 com/
│   │   │   │   └── 📂 volo/
│   │   │   │   │   └── 📂 broker/
│   │   │   │   │   │   ├── 📂 background/
│   │   │   │   │   │   │   ├── 📄 BackgroundTask.java
│   │   │   │   │   │   │   ├── 📄 BackgroundTaskScheduler.java
│   │   │   │   │   │   │   ├── 📄 ExampleCleanupTask.java
│   │   │   │   │   │   │   └── 📄 TaskSchedule.java
│   │   │   │   │   │   ├── 📄 BrokerApplication.java
│   │   │   │   │   │   ├── ⚙️ config/
│   │   │   │   │   │   │   └── 📄 Config.java
│   │   │   │   │   │   ├── 📂 engine/
│   │   │   │   │   │   │   ├── 📄 ConsumerChannel.java
│   │   │   │   │   │   │   ├── 📄 DefaultInFlightTracker.java
│   │   │   │   │   │   │   ├── 📄 DefaultSubscriptionManager.java
│   │   │   │   │   │   │   ├── 📄 DeliveryMode.java
│   │   │   │   │   │   │   ├── 📄 DispatchManager.java
│   │   │   │   │   │   │   ├── 📄 InFlightTracker.java
│   │   │   │   │   │   │   ├── 📄 InMemoryDequeManager.java
│   │   │   │   │   │   │   ├── 📄 InMemoryQueueManager.java
│   │   │   │   │   │   │   ├── 📄 MessageDelivery.java
│   │   │   │   │   │   │   ├── 📄 QueuedMessage.java
│   │   │   │   │   │   │   ├── 📄 QueueManager.java
│   │   │   │   │   │   │   ├── 📄 Subscription.java
│   │   │   │   │   │   │   └── 📄 SubscriptionManager.java
│   │   │   │   │   │   └── 📂 transport/
│   │   │   │   │   │   │   ├── 📂 grpc/
│   │   │   │   │   │   │   │   ├── 📄 GrpcConsumerChannelAdapter.java
│   │   │   │   │   │   │   │   ├── 📄 GrpcConsumerService.java
│   │   │   │   │   │   │   │   ├── 📄 GrpcMessageConverters.java
│   │   │   │   │   │   │   │   └── 📄 GrpcPublisherService.java
│   │   │   │   │   │   │   ├── 📂 model/
│   │   │   │   │   │   │   │   ├── 📄 AckCommand.java
│   │   │   │   │   │   │   │   ├── 📄 ConsumerCommand.java
│   │   │   │   │   │   │   │   ├── 📄 NackCommand.java
│   │   │   │   │   │   │   │   ├── 📄 PublishCommand.java
│   │   │   │   │   │   │   │   ├── 📄 PublishResult.java
│   │   │   │   │   │   │   │   ├── 📄 SafeGrpcObserver.java
│   │   │   │   │   │   │   │   └── 📄 SubscribeCommand.java
│   │   │   │   │   │   │   ├── 📄 TransportCommandRouter.java
│   │   │   │   │   │   │   └── 📄 TransportCommandRouterImpl.java
│   │   ├── 📂 proto/
│   │   │   └── 📄 broker.proto
│   │   └── 📂 resources/
│   │   │   └── ⚙️ application.yaml
│   └── 📂 test/
│   │   └── 📂 java/
│   │   │   └── 📂 com/
│   │   │   │   └── 📂 volo/
│   │   │   │   │   └── 📂 broker/
│   │   │   │   │   │   ├── 📂 background/
│   │   │   │   │   │   │   └── 📄 BackgroundTaskSchedulerTests.java
│   │   │   │   │   │   ├── 📂 engine/
│   │   │   │   │   │   │   └── 📄 QueueEngineTests.java
│   │   │   │   │   │   └── 📂 transport/
│   │   │   │   │   │   │   ├── 📂 grpc/
│   │   │   │   │   │   │   │   ├── 📄 GrpcConsumerChannelAdapterTest.java
│   │   │   │   │   │   │   │   ├── 📄 GrpcMessageConvertersTest.java
│   │   │   │   │   │   │   │   └── 📄 GrpcServicesTest.java
│   │   │   │   │   │   │   ├── 📄 TransportCommandRouterImplTest.java
│   │   │   │   │   │   │   └── 📄 TransportLayerIntegrationTest.java
└── 📖 TRANSPORT_LAYER_ARCHITECTURE.md
```

## 📖 Legend

### File Types
- 📄 Other: Other files
- 🚫 DevOps: Git ignore
- 📖 Docs: Markdown files
- 📄 Docs: Text files
- ⚙️ Config: XML files
- ⚙️ Config: YAML files

### Importance Levels
- 🔴 Critical: Essential project files
- 🟡 High: Important configuration files
- 🔵 Medium: Helpful but not essential files
