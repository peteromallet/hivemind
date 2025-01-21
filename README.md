# BNDC Engine

A Discord bot that generates comprehensive daily summaries of channel activities using Claude 3.5 Sonnet. The bot monitors specified channels and categories, processes messages, and creates organized summaries with media handling, thread management, and historical archiving capabilities. Lots more coming.

## Features

- 📊 Daily automated news summaries - both whole community and channel-specific
- 🗄️ Ongoing and historic archiving of all messages and media
- 🖌️ Art curation capabilities - assign curators to keep contributions high-quality.
- 🛠️ Development mode for testing with sample data

## Live Demo

Want to see it in action? Join the [Banodoco Discord server](https://discord.gg/NnFxGvx94b) to see the bot's daily summaries and features live!

## Setup

### Installation

1. Clone the repository:
```bash
git clone https://github.com/peteromallet/bndc-engine.git
cd bndc-engine

```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with the following variables:
```
# Required Bot Configuration
DISCORD_BOT_TOKEN=your_discord_bot_token
ANTHROPIC_API_KEY=your_anthropic_api_key

# Main Configuration
GUILD_ID=your_server_id
PRODUCTION_SUMMARY_CHANNEL_ID=channel_id_for_summaries
CATEGORIES_TO_MONITOR=category_id1,category_id2
ADMIN_USER_ID=your_admin_user_id

# Development Configuration (optional)
DEV_GUILD_ID=dev_server_id
DEV_SUMMARY_CHANNEL_ID=dev_channel_id
DEV_CATEGORIES_TO_MONITOR=dev_category_ids
TEST_DATA_CHANNEL=test_channel_id

```

### Running the Bot

Basic operation:
```bash
python main.py
```

Development mode:
```bash
python main.py --dev
```

Run summary immediately:
```bash
python main.py --run-now
```

### Bot Permissions

The bot requires the following Discord permissions:
- Read Messages/View Channels
- Send Messages
- Create Public Threads
- Send Messages in Threads
- Manage Messages (for pinning)
- Read Message History
- Attach Files
- Add Reactions
- View Channel
- Manage Threads

### Development Mode

Run the bot in development mode to:
- Use test data instead of live channels
- Test in a development server
- Avoid affecting production data

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

Archive management commands:
```bash
# Archive specific channels or date ranges
python scripts/archive_discord.py --channel-id <channel_id> --start-date YYYY-MM-DD

# Clean up test or temporary data
python scripts/cleanup_test_data.py

# Migrate database schema
python scripts/migrate_db.py
```
```