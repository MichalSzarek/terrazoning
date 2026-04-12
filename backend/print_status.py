"""Operational DB status snapshot for TerraZoning."""

from __future__ import annotations

import asyncio

from sqlalchemy import text

from app.core.database import AsyncSessionLocal

SQL = text(
    """
    SELECT 'bronze.raw_listings' AS metric, COUNT(*)::text AS value FROM bronze.raw_listings
    UNION ALL
    SELECT 'bronze.pending', COUNT(*)::text FROM bronze.raw_listings WHERE is_processed = false
    UNION ALL
    SELECT 'silver.dzialki', COUNT(*)::text FROM silver.dzialki
    UNION ALL
    SELECT 'silver.dlq_parcels', COUNT(*)::text FROM silver.dlq_parcels
    UNION ALL
    SELECT 'gold.planning_zones', COUNT(*)::text FROM gold.planning_zones
    UNION ALL
    SELECT 'gold.investment_leads', COUNT(*)::text FROM gold.investment_leads
    ORDER BY metric
    """
)


async def main() -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(SQL)
        for metric, value in result.fetchall():
            print(f"{metric:22s} {value}")


if __name__ == "__main__":
    asyncio.run(main())
