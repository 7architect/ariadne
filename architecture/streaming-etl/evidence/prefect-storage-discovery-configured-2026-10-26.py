import asyncio
import gc
import tracemalloc
import warnings
from prefect import flow, task
from prefect.filesystems import LocalFileSystem
from prefect.settings import temporary_settings
import test_postgres_prefect_adapter as fixtures

@task
async def passthrough(value):
    return value

@flow
async def protocol_probe():
    return await passthrough(7)

async def main():
    fixture = fixtures.PostgresPrefectAttemptTests('runTest')
    fixture.admin_token = 'local-probe-only'
    resources = await fixture.start_prefect_server()
    try:
        warnings.simplefilter('default', ResourceWarning)
        storage = LocalFileSystem(basepath=resources[1].name + "/results")
        await storage.asave("explicit-probe-storage", overwrite=True)
        selected = temporary_settings(updates={"PREFECT_RESULTS_DEFAULT_STORAGE_BLOCK": "local-file-system/explicit-probe-storage"})
        selected.__enter__()
        tracemalloc.start(8)
        state = await protocol_probe(return_state=True)
        assert await state.result() == 7
        gc.collect()
        await asyncio.sleep(.1)
        gc.collect()
        tracemalloc.stop()
        selected.__exit__(None, None, None)
    finally:
        await fixture.stop_prefect_server(resources)
    print('TINY_PREFECT_PROBE_DONE')

asyncio.run(main())
