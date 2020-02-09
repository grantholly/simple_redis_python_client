#!/usr/bin/env python3.8
import asyncio


class Client:
    '''
    While the connect method creates a single reader and
    a single writer, it is not safe for multiple coroutines
    to use them.  When calls are scheduled, any runnable
    coroutine can interrupt another coroutine sending a command
    or reading a reply.  As a result, this is not safe to use
    the client.
    '''

    async def connect(self, host, port):
        self.r, self.w = await asyncio.open_connection(host, port)

    async def get(self, key):
        self.w.write(f"GET {key}\r\n".encode())
        await self.w.drain()

        return await self._reply()

    async def set(self, key, val):
        self.w.write(f"SET {key} {val}\r\n".encode())
        await self.w.drain()

        return await self._reply()

    async def incr(self, key):
        self.w.write(f"INCR {key}\r\n".encode())
        await self.w.drain()

        return await self._reply()

    async def send(self, *args):
        '''
        generic to send any command
        '''
        resp_args = "".join([f"${len(arg)}\r\n{arg}\r\n" for arg in args])
        self.w.write(f"*{len(args)}\r\n{resp_args}".encode())
        await self.w.drain()

        return await self._reply()

    async def _reply(self):
        tag = await self.r.read(1)

        # https://redis.io/topics/protocol
        #
        # Integers
        if tag == b":":
            result = b""
            ch = b""

            while ch != b"\n":
                ch = await self.r.read(1)
                result += ch
            return int(result[:-1].decode())

        # Errors
        if tag == b"-":
            result = b""
            ch = b""

            while ch != b"\n":
                ch = await self.r.read(1)
                result += ch
            raise Exception(result[:-1].decode())

        # Bulk Strings
        if tag == b"$":
            length = b""
            result = b""
            ch = b""

            while ch != b"\n":
                ch = await self.r.read(1)
                length += ch

            data_length = int(length[:-1]) + 2

            while len(result) < data_length:
                result += await self.r.read(data_length - len(result))

            return result[:-2].decode()

        # Simple Strings
        if tag == b"+":
            result = b""
            ch = b""

            while ch != b"\n":
                ch = await self.r.read(1)
                result += ch
            return result[:-1].decode()

        # Fallthrough to Exception
        else:
            msg = await self.r.read(100)
            raise Exception(f"Unkown tag: {tag}, msg: {msg.decode()}")

async def main():
    print("Asyncio Python Redis Client")
    client = Client()
    await client.connect("localhost", 6379)
    print(await client.set("first", 1))
    print(await client.send("set", "third", "way after first"))
    print(await client.get("first"))
    print(await client.incr("first"))

if __name__ == "__main__":
    asyncio.run(main())
