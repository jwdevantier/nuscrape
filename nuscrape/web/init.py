import logging
from .app import app
from .routers import scrapers

log = logging.getLogger(__name__)

# only export the app itself
__all__ = ["app"]

# wire up routes
app.include_router(scrapers.router, prefix="/scrapers", tags=["scrapers"])


# wire events for starting and stopping
async def on_startup():
    log.info("server starting up...")


async def on_shutdown():
    log.info("server shutting down...")


app.on_event("startup")(on_startup)
app.on_event("shutdown")(on_shutdown)
