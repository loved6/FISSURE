#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""Standalone test for Operation subclasses
"""
import asyncio
import logging

async def test_op(cls, parameters: dict, resource_args: dict = {}, logger: logging.Logger = logging.getLogger('OperationTest')) -> None:
    """Run the operation under test.

    Parameters
    ----------
    cls : type
        The Operation subclass to test.
    parameters : dict
        Arguments to pass to the class constructor.
    resource_args : dict, optional
        Arguments to pass to the get_resources method, by default {}
    logger : logging.Logger, optional
        Logger instance, by default logging.getLogger('OperationTest')
    """
    logger.setLevel(logging.DEBUG)

    # display parameters, resources, and interfaces
    params = cls.get_arguments(logger)
    logger.info('\nClass Parameters:')
    for k, v in params.items():
        logger.info(f"'{k}': {v}")
    logger.info('\n\n')

    logger.info("\nInput Parameters:")
    for k, v in parameters.items():
        logger.info(f"'{k}': {v}")
    logger.info('\n\n')

    resources = cls.get_resources(**resource_args)
    logger.info('\nClass Resources:')
    for k, v in resources.items():
        logger.info(f"'{k}': {v}")
    logger.info('\n\n')

    interfaces = cls.get_interfaces()
    logger.info('\nClass Interfaces:')
    for k, v in interfaces.items():
        logger.info(f"'{k}': {v}")
    logger.info('\n\n')

    # add logger to parameters
    parameters['logger'] = logger

    # create operation instance
    logger.info(f"Creating operation instance of {cls.__name__}")
    op = cls(**parameters)
    status = await op.setup()
    if not status:
        logger.error("Setup failed. Exiting.")
        return
    logger.info("Setup complete. Running...")
    try:
        # Run the operation until interrupted (e.g., Ctrl+C)
        asyncio.create_task(op.run())
        logger.debug("Operation run task created.")
        while op.running() is None:
            logger.debug("Waiting for operation to start...")
            await asyncio.sleep(1)
        if not op.running():
            logger.error("Operation did not start successfully.")
            await op.stop()
            await op.teardown()
            return
        logger.info("Operation running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)
    except asyncio.TimeoutError:
        logger.warning("Timeout reached. Stopping operation...")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Stopping operation...")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        logger.info("Stopping operation...")
        await op.stop()
        await op.teardown()
        logger.info("Operation stopped and torn down.")

def run_test(cls, class_args: dict, resource_args: dict = {}, logger: logging.Logger = logging.getLogger('OperationTest')) -> None:
    """Run the test operation in an asyncio event loop.
    """
    with asyncio.Runner() as runner:
        runner.run(test_op(cls, class_args, resource_args, logger))