#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  7 17:51:55 2019

@author: sam
"""
import asyncio
from signal import SIGTERM,SIGINT

# just some boilerplate code to do the same thing for SIGTERM and SIGINT
def handler(sig):
    loop.stop()
    #print(f'Got signal: {sig!s}, shutting down.')
    loop.remove_signal_handler(SIGTERM)
    loop.add_signal_handler(SIGINT, lambda: None)

# watch out, the following code runs upon importing the module
loop = asyncio.get_event_loop()
for sig in (SIGTERM, SIGINT):
    loop.add_signal_handler(sig, handler, sig)


    
# the main function, i.e. the main task that will be run untill completion
#async def main(logfile):
async def bigLoop(interval,funcToDo):
    if (interval<0.01): # some arbitrary speed limit imposed here.
        interval=0.01

    try:
        while True:
            # principle of loop: start timer but don't wait
            # then perform other tasks and wait for them to finish
            # in the end, block until time's up.
            timer=asyncio.sleep(interval) # start the timer
            funcToDo()
            await timer                  # only continue loop when time's up

            
    except asyncio.CancelledError:
        print('Shutting down')
        await asyncio.sleep(1) # just to make sure everything is switched off, not necessary

def doIt(interv_func_list):
    for tt in interv_func_list:
        loop.create_task(bigLoop(tt[0],tt[1]))
    loop.run_forever()
    
    tasks = asyncio.Task.all_tasks()
    for t in tasks:
        t.cancel()
    group = asyncio.gather(*tasks, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()


