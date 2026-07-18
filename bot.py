import os
import mimetypes
from pyrogram import Client, filters
from pyrogram.types import Message
from aiohttp import web

# Environment variables from hosting platform
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "0")) # A channel ID where files are stored
PORT = int(os.environ.get("PORT", "8080"))
FQDN = os.environ.get("FQDN", "") # Your app URL, e.g., https://your-bot.koyeb.app

app = Client("stream_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
routes = web.RouteTableDef()

@app.on_message(filters.private & (filters.document | filters.video))
async def handle_incoming_file(client, message: Message):
    # Forward the file to the Bin Channel to keep it safe and online
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
    
    # Read Range headers for seek-bar compatibility (HTML5 Video requirement)
    range_header = request.headers.get('Range')
    start, end = 0, file_size - 1

    if range_header:
        # Example: bytes=200-1000
        ranges = range_header.replace('bytes=', '').split('-')
        if ranges[0]:
            start = int(ranges[0])
        if len(ranges) > 1 and ranges[1]:
            end = int(ranges[1])

    chunk_size = 1024 * 1024 # 1MB chunk sizes
    response = web.StreamResponse(status=206 if range_header else 200)
    response.headers['Content-Type'] = mime_type
    response.headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
    response.headers['Content-Length'] = str(end - start + 1)
    response.headers['Accept-Ranges'] = 'bytes'
    
    await response.prepare(request)

    # Stream chunks from Telegram directly to the web app player
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

    async for chunk in chunk_generator():
        await response.write(chunk)

    return response

async def start_web_server():
    server = web.Application()
    server.add_routes(routes)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

if __name__ == "__main__":
    app.start()
    app.loop.run_until_complete(start_web_server())
    print("Bot & Web Server running seamlessly...")
    import idle
    idle.idle()
    app.stop()