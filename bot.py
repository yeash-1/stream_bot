 import os
import mimetypes
import asyncio
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from aiohttp import web

# Environment variables from hosting platform
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "0"))
PORT = int(os.environ.get("PORT", "8080"))
FQDN = os.environ.get("FQDN", "")

app = Client("stream_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
routes = web.RouteTableDef()

@app.on_message(filters.private & (filters.document | filters.video))
async def handle_incoming_file(client, message: Message):
    # Forward the file to the Bin Channel securely
    forwarded = await message.forward(BIN_CHANNEL)
    msg_id = forwarded.id
    
    # Generate the custom direct stream link
    stream_link = f"{FQDN}/stream/{msg_id}"
    
    await message.reply_text(
        f"**File Stored Successfully!**\n\n"
        f"**Direct Stream Link:**\n`{stream_link}`",
        disable_web_page_preview=True
    )

@routes.get("/stream/{msg_id}")
async def stream_handler(request):
    try:
        msg_id = int(request.match_info['msg_id'])
    except ValueError:
        return web.Response(text="Invalid Message ID", status=400)

    try:
        # Fetch the message from Bin Channel
        message = await app.get_messages(BIN_CHANNEL, msg_id)
    except Exception as e:
        return web.Response(text="File Not Found", status=404)

    media = message.document or message.video
    if not media:
        return web.Response(text="Invalid Media Type", status=400)

    file_size = media.file_size
    mime_type = media.mime_type or mimetypes.guess_type(media.file_name)[0] or 'application/octet-stream'
    
    range_header = request.headers.get('Range')
    start, end = 0, file_size - 1

    if range_header:
        ranges = range_header.replace('bytes=', '').split('-')
        if ranges[0]:
            start = int(ranges[0])
        if len(ranges) > 1 and ranges[1]:
            end = int(ranges[1])

    chunk_size = 1024 * 1024  # 1MB chunk sizes
    response = web.StreamResponse(status=206 if range_header else 200)
    response.headers['Content-Type'] = mime_type
    response.headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
    response.headers['Content-Length'] = str(end - start + 1)
    response.headers['Accept-Ranges'] = 'bytes'
    
    await response.prepare(request)

    async def chunk_generator():
        offset = start
        while offset <= end:
            current_end = min(offset + chunk_size - 1, end)
            chunk = await app.download_media(
                media,
                in_memory=True,
                block=False,
                offset=offset,
                limit=current_end - offset + 1
            )
            if not chunk:
                break
            yield chunk
            offset += len(chunk)

    async def write_to_response():
        async for chunk in chunk_generator():
            await response.write(chunk)

    try:
        await write_to_response()
    except Exception as e:
        pass

    return response

async def main():
    # Asynchronously start Pyrogram Client
    await app.start()
    
    # Configure and start Aiohttp Web Server
    server = web.Application()
    server.add_routes(routes)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"Server is running on port {PORT}...")
    
    # Keep running until terminated
    await idle()
    
    # Asynchronously stop Pyrogram Client
    await app.stop()

if __name__ == "__main__":
    # Async Event Loop Runner
    asyncio.get_event_loop().run_until_complete(main())
