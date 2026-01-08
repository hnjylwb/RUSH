#!/usr/bin/env python3
"""
VM Worker Startup Script

Run this on VM executor nodes to start the query execution service.
The worker will listen for query requests and execute them using DuckDB.
"""

import argparse
import asyncio
from ease.vm_worker import VMWorker


async def main():
    parser = argparse.ArgumentParser(description='Start VM Worker for query execution')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='Host address to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8081,
                        help='Port to listen on (default: 8081)')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Directory containing data files')

    args = parser.parse_args()

    # Create and start VM worker
    worker = VMWorker(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir
    )

    try:
        await worker.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
