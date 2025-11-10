**DBSyncService**

.NET Core Worker Service for Real-Time MSSQL to Redis Synchronization

A high-performance .NET Core Worker Service that synchronizes multiple MSSQL databases with Redis in real-time. Supports multiple connection strings for both MSSQL and Redis, using batch processing for efficient, consistent data synchronization.

**Features**
✅ Supports multiple MSSQL connection strings
✅ Supports multiple Redis connection strings
✅ Real-time synchronization from relational databases to Redis
✅ Batch processing for high efficiency and low latency
✅ Runs as a background worker service
✅ Configurable and scalable for enterprise workloads

**Prerequisites**

1. .NET 6.0 SDK or later
2. Microsoft SQL Server
3. Redis Server

**Installation & Setup**
1. git clone https://github.com/AbhayUpadhyayDev/DBSync.git
2. cd DBSyncService
3. Configure appsettings.json with your database and Redis connections
4. Build and run the service:

**Usage**

1. The worker service runs in the background and automatically syncs MSSQL data to Redis.
2. Supports real-time updates with configurable batch size.
3. Can be deployed as a Windows Service or Linux daemon.
