import asyncio
import copy
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import discord
import bot
import http_client
import personal_display
import yummy_api


class Message:
    def __init__(self, mid, embed):
        self.id = mid
        self.embeds = [embed]
        self.edit = AsyncMock(side_effect=self.update)
        self.delete = AsyncMock()

    def update(self, **kwargs):
        self.embeds = [kwargs['embed']]


class DisplayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pl = {'thread_id': 10, 'keys': ['a', 'b'], 'display_mode': 'summary', 'recent_keys': ['b']}
        self.state = {'personal_lists': {'1': {'2': self.pl}}}
        self.messages = {}
        self.thread = SimpleNamespace(archived=False, locked=False, fetch_message=AsyncMock(side_effect=self.fetch),
                                      send=AsyncMock(side_effect=self.send))
        self.client = SimpleNamespace(get_guild=lambda _: SimpleNamespace(get_member=lambda _: SimpleNamespace(display_name='Name')),
                                      get_channel=lambda _: self.thread)

    async def fetch(self, mid):
        if mid not in self.messages:
            raise discord.NotFound(SimpleNamespace(status=404, reason='missing'), 'missing')
        return self.messages[mid]

    async def send(self, **kwargs):
        self.assertTrue(kwargs['silent'])
        self.assertEqual(kwargs['allowed_mentions'].to_dict()['parse'], [])
        mid = len(self.messages) + 1
        self.messages[mid] = Message(mid, kwargs['embed'])
        return self.messages[mid]

    async def write(self, uid, **kwargs):
        self.pl.update(copy.deepcopy(kwargs))

    async def render(self, incremental=False):
        with patch.object(personal_display.discord, 'Thread', SimpleNamespace):
            await personal_display.rebuild_display(self.client, 1, 2, session=None,
                incremental=incremental, read_state=AsyncMock(return_value=self.state),
                write_personal_fields=self.write, title_for_key=lambda s,k:k,
                jump_for_key=lambda s,g,k:'https://example.org/'+k,
                ordered_keys=lambda p:p['keys'], fetch_meta=AsyncMock(return_value={'title':'Anime'}),
                build_card_embed=lambda *a,**kw:discord.Embed(title=str(a[3])),
                hub_embed_builder=lambda p,n:discord.Embed(title=n),
                hub_view_factory=lambda:None, accent_palette=(), default_accent=1)

    async def test_summary_refresh_does_not_send_or_delete(self):
        await self.render()
        self.assertEqual(self.thread.send.await_count, 3)
        await self.render(incremental=True)
        await self.render()
        self.assertEqual(self.thread.send.await_count, 3)
        for msg in self.messages.values():
            msg.delete.assert_not_awaited()

    async def test_gallery_refresh_and_new_item(self):
        self.pl['display_mode'] = 'gallery'
        await self.render()
        old = dict(self.pl['anime_messages'])
        await self.render()
        self.assertEqual(self.pl['anime_messages'], old)
        self.pl['keys'].append('c')
        await self.render()
        self.assertEqual(self.thread.send.await_count, 4)

    async def test_paging_reuses_message(self):
        self.pl.update(display_mode='paged', keys=[str(i) for i in range(45)])
        await self.render()
        mid = self.pl['page_message_id']
        self.pl['current_page'] = 1
        await self.render(incremental=True)
        self.assertEqual(self.pl['page_message_id'], mid)
        self.assertEqual(self.thread.send.await_count, 3)
        self.assertIn('21.', self.messages[mid].embeds[0].description)

    async def test_forbidden_does_not_duplicate(self):
        await self.render()
        self.thread.fetch_message.side_effect = discord.Forbidden(SimpleNamespace(status=403, reason='Forbidden'), 'no')
        with self.assertRaises(discord.Forbidden):
            await self.render()
        self.assertEqual(self.thread.send.await_count, 3)

    async def test_missing_message_replaced_once(self):
        await self.render()
        del self.messages[self.pl['recent_message_id']]
        await self.render()
        await self.render()
        self.assertEqual(self.thread.send.await_count, 4)


class Response:
    def __init__(self, status, data=None, headers=None):
        self.status, self.data, self.headers = status, data, headers or {}
    async def __aenter__(self): return self
    async def __aexit__(self, *args): return False
    async def json(self, **kwargs):
        if isinstance(self.data, Exception): raise self.data
        return self.data


class APITests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_migration_rejects_channels_from_another_guild(self):
        channel=SimpleNamespace(guild=SimpleNamespace(id=999))
        with patch.dict(os.environ,{'DISCORD_GUILD_ID':'123'}), patch.object(bot,'read_state_copy',AsyncMock(return_value={'guilds':{}})), patch.object(bot.bot,'get_channel',return_value=channel), patch.object(bot.discord,'ForumChannel',SimpleNamespace), patch.object(bot,'_write_state') as write:
            await bot.migrate_legacy_guild_config()
        write.assert_not_called()

    async def test_setup_twice_does_not_create_duplicate_forums(self):
        import guild_config
        config={}
        guild=SimpleNamespace(id=813, get_channel=lambda cid:SimpleNamespace(guild=SimpleNamespace(id=813)))
        async def save(gid,cfg): config.update(cfg)
        async def read(gid): return config or None
        create=AsyncMock(return_value=({'forum_channel_id':1,'list_forum_channel_id':2},None))
        with patch.object(bot,'get_guild_cfg',side_effect=read), patch.object(bot,'save_guild_cfg',side_effect=save), patch.object(guild_config,'setup_guild_channels',create), patch.object(bot.discord,'ForumChannel',SimpleNamespace):
            first=await bot.ensure_guild_setup(guild)
            second=await bot.ensure_guild_setup(guild)
        self.assertIsNone(first[1])
        self.assertIsNone(second[1])
        create.assert_awaited_once()

    async def test_setup_forbidden_keeps_existing_binding(self):
        import guild_config
        cfg={'forum_channel_id':1,'list_forum_channel_id':2}
        guild=SimpleNamespace(id=814,get_channel=lambda _:None,fetch_channel=AsyncMock(side_effect=discord.Forbidden(SimpleNamespace(status=403,reason='Forbidden'),'no')))
        with patch.object(bot,'get_guild_cfg',AsyncMock(return_value=cfg)), patch.object(guild_config,'setup_guild_channels',AsyncMock()) as create:
            result,error=await bot.ensure_guild_setup(guild)
        self.assertEqual(result,cfg)
        self.assertIsNotNone(error)
        create.assert_not_awaited()

    async def test_restore_archived_buttons_without_editing_or_sending(self):
        state={'threads':{'21':{'rating_message_id':30,'recommend_message_id':31}}}
        messages={mid:SimpleNamespace(id=mid,author=SimpleNamespace(id=99),components=[SimpleNamespace(children=[SimpleNamespace(custom_id='old-'+str(mid))])],edit=AsyncMock()) for mid in (30,31)}
        channel=SimpleNamespace(fetch_message=AsyncMock(side_effect=lambda mid:messages[mid]),send=AsyncMock())
        from unittest.mock import Mock
        client=SimpleNamespace(user=SimpleNamespace(id=99),get_channel=lambda _:channel,add_view=Mock())
        with patch.object(bot,'read_state_copy',AsyncMock(return_value=state)), patch.object(bot,'_load_state',return_value=state), patch.object(bot,'_write_state'), patch.object(bot.discord,'Thread',SimpleNamespace):
            await bot.restore_legacy_topic_panels(client)
            await bot.restore_legacy_topic_panels(client)
        for message in messages.values():message.edit.assert_not_awaited()
        channel.send.assert_not_awaited()
        self.assertEqual(state['threads']['21']['persistent_panels_version'],2)
        self.assertEqual(state['threads']['21']['rating_message_id_custom_id'],'old-30')
        self.assertEqual(client.add_view.call_args_list[0].args[0].children[0].custom_id,'old-30')

    async def test_requested_write_opens_unlocked_archive(self):
        from thread_utils import ensure_thread_writable
        thread=SimpleNamespace(archived=True,locked=False,edit=AsyncMock())
        await ensure_thread_writable(thread)
        self.assertFalse(thread.edit.call_args.kwargs['archived'])

    async def test_requested_write_does_not_unlock_moderated_thread(self):
        from thread_utils import ensure_thread_writable
        thread=SimpleNamespace(archived=True,locked=True,edit=AsyncMock())
        self.assertIs(await ensure_thread_writable(thread),thread)
        thread.edit.assert_not_awaited()

    async def test_rating_acknowledges_before_panel_request(self):
        events=[]
        async def defer(**kwargs):events.append('defer')
        async def refresh(*args):events.append('refresh')
        modal=bot.AnimeRatingModal(1)
        modal.score._value='8'
        interaction=SimpleNamespace(user=SimpleNamespace(id=2),client=None,response=SimpleNamespace(defer=AsyncMock(side_effect=defer)),followup=SimpleNamespace(send=AsyncMock()))
        with patch.object(bot,'set_user_rating',AsyncMock()), patch.object(bot,'refresh_rating_panel',AsyncMock(side_effect=refresh)):
            await modal.on_submit(interaction)
        self.assertEqual(events,['defer','refresh'])

    async def test_retry_429_then_success(self):
        from unittest.mock import Mock
        session = SimpleNamespace(get=Mock(side_effect=[Response(429, headers={'Retry-After':'0'}), Response(200, {'ok':True})]))
        with patch.object(http_client.asyncio, 'sleep', new_callable=AsyncMock):
            data, status = await http_client.get_json(session, 'https://example.org')
        self.assertEqual((data, status), ({'ok':True}, 200))
        self.assertEqual(session.get.call_count, 2)

    async def test_no_retry_auth_failure(self):
        from unittest.mock import Mock
        session = SimpleNamespace(get=Mock(return_value=Response(401)))
        self.assertEqual(await http_client.get_json(session, 'https://example.org'), (None,401))
        self.assertEqual(session.get.call_count, 1)

    async def test_invalid_json_does_not_escape(self):
        from unittest.mock import Mock
        session = SimpleNamespace(get=Mock(return_value=Response(200, ValueError('html'))))
        self.assertEqual(await http_client.get_json(session, 'https://example.org'), (None,200))

    async def test_retries_are_bounded(self):
        from unittest.mock import Mock
        session = SimpleNamespace(get=Mock(return_value=Response(503)))
        with patch.object(http_client.asyncio, 'sleep', new_callable=AsyncMock):
            self.assertEqual(await http_client.get_json(session, 'https://example.org'), (None,503))
        self.assertEqual(session.get.call_count, 3)

    async def test_malformed_list_is_error(self):
        with patch.object(yummy_api, '_yani_request', AsyncMock(return_value=({'response':{}},200))):
            entries, token, error = await yummy_api.yani_fetch_lists_with_token_refresh(None,'app','token',1,'test')
        self.assertIsNone(entries)
        self.assertIsNotNone(error)

    async def test_rotated_token_survives_failed_list_read(self):
        with patch.object(yummy_api, 'yani_get_user_lists', AsyncMock(side_effect=[(None,401),(None,503)])), patch.object(yummy_api,'yani_refresh_access_token',AsyncMock(return_value='new')):
            _, token, error = await yummy_api.yani_fetch_lists_with_token_refresh(None,'app','old',1,'test')
        self.assertEqual(token,'new')
        self.assertIsNotNone(error)

    async def test_import_persists_rotation_before_reporting_error(self):
        state={'yummy_accounts':{'2':{'yummy_user_id':4,'access_token':'old'}}}
        with patch.object(bot, 'read_state_copy', AsyncMock(return_value=state)), patch.object(yummy_api, 'yani_fetch_lists_with_token_refresh', AsyncMock(return_value=(None,'new','unavailable'))), patch.object(bot, 'update_yummy_access_token', AsyncMock()) as save:
            result=await bot.run_yummy_list_import_for_member(SimpleNamespace(id=1),2,list_filter='all',max_topics=1,session=None,app_token='app')
        save.assert_awaited_once_with(2,'new')
        self.assertEqual(result['error'],'unavailable')

    async def test_mal_later_page_failure_not_reported_as_success(self):
        with patch.object(bot,'mal_fetch_list_page',AsyncMock(side_effect=[([{}]*50,200),([],503)])), patch.object(bot.asyncio,'sleep',AsyncMock()):
            data, status=await bot.mal_fetch_full_list(None,'name',7)
        self.assertEqual((data,status),([],503))

    async def test_opt_in_alias_calls_command_callback(self):
        import register_commands
        client=bot.YummyBot()
        with patch.dict(os.environ, {'DISCORD_LEGACY_COMMANDS':'1'}):
            register_commands.setup(client)
        canonical=client.tree.get_command('list').get_command('show')
        with patch.object(canonical,'_callback',AsyncMock()) as callback:
            interaction=SimpleNamespace()
            await client.tree.get_command('mylist').callback(interaction,None)
            callback.assert_awaited_once_with(interaction,None)
        await client.close()

    async def test_persistent_views(self):
        self.assertTrue(bot.RateAnimePanelView(thread_id=1).is_persistent())
        self.assertTrue(bot.RecommendPanelView(thread_id=1).is_persistent())

    async def test_serializes_same_catalog(self):
        active = 0
        maximum = 0
        @bot.catalog_serialized
        async def operation(guild):
            nonlocal active, maximum
            active += 1
            maximum = max(active, maximum)
            await asyncio.sleep(0)
            active -= 1
        await asyncio.gather(operation(SimpleNamespace(id=998)), operation(SimpleNamespace(id=998)))
        self.assertEqual(maximum,1)


class StateTests(unittest.TestCase):
    def test_owner_requires_id_not_matching_name(self):
        with patch.dict(os.environ,{'DISCORD_BOT_OWNER_ID':'123'}):
            self.assertFalse(bot.is_bot_owner(SimpleNamespace(id=999,name=bot.BOT_OWNER_USERNAME)))
            self.assertTrue(bot.is_bot_owner(SimpleNamespace(id=123,name='renamed')))

    def test_corruption_never_becomes_empty_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'state.json'
            path.write_text('{broken')
            with patch.object(bot,'DATA_DIR',Path(directory)), patch.object(bot,'STATE_PATH',path):
                with self.assertRaises(RuntimeError): bot._load_state()
            self.assertEqual(path.read_text(),'{broken')

    def test_no_aliases_by_default(self):
        names=[command.name for command in bot.bot.tree.get_commands()]
        self.assertEqual(len(names),len(set(names)))
        self.assertIn('anime',names)
        self.assertNotIn('aa',names)
        self.assertNotIn('mylist',names)


if __name__ == '__main__':
    unittest.main()
