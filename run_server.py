#!/usr/bin/env python3
"""
Start the query scheduling server
"""

import asyncio
import argparse
from ease.server import SchedulingServer


async def main():
    parser = argparse.ArgumentParser(description="Query Scheduling Server")
    parser.add_argument('--config', default='config',
                       help='Configuration directory (default: config)')
    parser.add_argument('--resources', default=None,
                       help='CSV file with query resource requirements')
    parser.add_argument('--host', default='0.0.0.0',
                       help='Server host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8080,
                       help='Server port (default: 8080)')

    args = parser.parse_args()

    # Create and start server
    server = SchedulingServer(
        config_dir=args.config,
        resource_csv=args.resources,
        host=args.host,
        port=args.port
    )

    try:
        await server.start()
    except KeyboardInterrupt:
        print("\n\nReceived shutdown signal")
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
