package com.github.wekite.features.items.chat

import android.content.ContentValues
import androidx.activity.ComponentActivity
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.ListItem
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.SegmentedButton
import androidx.compose.material3.SegmentedButtonDefaults
import androidx.compose.material3.SingleChoiceSegmentedButtonRow
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.github.wekite.BuildConfig
import com.github.wekite.features.api.core.WeApi
import com.github.wekite.features.api.core.WeDatabaseApi
import com.github.wekite.features.api.core.WeDatabaseListenerApi
import com.github.wekite.features.api.core.models.MessageInfo
import com.github.wekite.features.api.core.models.MessageType
import com.github.wekite.features.core.ClickableFeature
import com.github.wekite.features.core.Feature
import com.github.wekite.preferences.WePrefs
import com.github.wekite.ui.content.AlertDialogContent
import com.github.wekite.ui.content.Button
import com.github.wekite.ui.content.DefaultColumn
import com.github.wekite.ui.content.TextButton
import com.github.wekite.ui.content.dialogListItemColors
import com.github.wekite.ui.utils.showComposeDialog
import com.github.wekite.utils.WeLogger
import com.github.wekite.utils.android.showToast
import com.github.wekite.utils.strings.isGroupChatWxId
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.util.LinkedHashSet
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 消息实时上报：监听 message 表新消息插入（联系人/群聊），把能拿到的字段
 * 组装成 JSON，通过 POST 转发到用户配置的接口。
 *
 * - 复用 WeDatabaseListenerApi（数据库写入层 Hook）：所有会话的新消息都落在
 *   同一张 message 表，落库即触发，天然覆盖全部单聊与群聊，无需逐群注册。
 * - 解析/查显示名/网络全部在独立后台单线程执行，不占用微信 DB 线程与主线程。
 * - 开启后请在设置里点开本功能配置上报地址（保存即发送一条测试消息）。
 */
@Feature(
    name = "消息实时上报",
    categories = ["聊天"],
    description = "实时监听联系人/群聊消息，将可获取的信息通过 POST 上报到指定接口"
)
object RealTimeMessageReporter : ClickableFeature(), WeDatabaseListenerApi.IInsertListener {

    private const val TAG = "RealTimeMessageReporter"

    private const val MODE_ALL = "all"
    private const val MODE_CONTACT = "contact"
    private const val MODE_GROUP = "group"

    private const val MAX_PENDING = 512
    private const val RAW_CONTENT_MAX = 20000

    // ------------------------------------------------------------------
    // 配置（WePrefs / MMKV，设置对话框里修改）
    // ------------------------------------------------------------------
    private var reportUrl by WePrefs.prefOption("rt_report_url", "")
    private var reportMode by WePrefs.prefOption("rt_report_mode", MODE_ALL)
    private var includeSelf by WePrefs.prefOption("rt_report_include_self", false)
    private var onlyText by WePrefs.prefOption("rt_report_only_text", false)
    private var reportToken by WePrefs.prefOption("rt_report_token", "")

    private val jsonMediaType = "application/json; charset=utf-8".toMediaType()

    private val httpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .build()
    }

    private var worker: ExecutorService? = null
    private val pendingCount = AtomicInteger(0)
    private val seenKeys = LinkedHashSet<Long>()
    private val lastMissingUrlLogAt = AtomicLong(0)

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------
    override fun onEnable() {
        WeDatabaseListenerApi.addListener(this)
        if (worker == null) {
            worker = Executors.newSingleThreadExecutor { r ->
                Thread(r, "MsgReporter").apply { isDaemon = true }
            }
        }
        WeLogger.i(TAG, "enabled, mode=$reportMode, url=${reportUrl.take(64)}")
    }

    override fun onDisable() {
        WeDatabaseListenerApi.removeListener(this)
        worker?.shutdownNow()
        worker = null
        pendingCount.set(0)
        synchronized(seenKeys) { seenKeys.clear() }
    }

    // ------------------------------------------------------------------
    // 数据库监听入口（在微信 DB 线程回调，必须极快，只做策略过滤 + 入队）
    // ------------------------------------------------------------------
    override fun onInsert(table: String, values: ContentValues) {
        if (table != "message") return

        val type = values.getAsInteger("type") ?: return
        val isSend = values.getAsInteger("isSend") ?: 0
        val talker = values.getAsString("talker").orEmpty()

        // 策略过滤（全部为轻量标量判断，不在此线程做任何 DB 查询/网络）
        when (reportMode) {
            MODE_GROUP -> if (!talker.isGroupChatWxId) return
            MODE_CONTACT -> if (talker.isGroupChatWxId) return
        }
        if (!includeSelf && isSend != 0) return
        if (onlyText && MessageType.fromCode(type)?.isText != true) return

        // 只上报有稳定 msgSvrId 的消息，避免本地中间态/占位行
        val key = values.getAsLong("msgSvrId") ?: 0L
        if (key == 0L) return

        val worker = worker ?: return
        if (reportUrl.isBlank()) {
            val now = System.currentTimeMillis()
            val last = lastMissingUrlLogAt.get()
            if (now - last > 60_000L && lastMissingUrlLogAt.compareAndSet(last, now)) {
                WeLogger.w(TAG, "消息实时上报已开启但未配置上报地址")
            }
            return
        }

        val msg = runCatching { MessageInfo.fromContentValues(values) }.getOrNull() ?: return

        if (pendingCount.get() >= MAX_PENDING) {
            WeLogger.w(TAG, "pending queue full, drop one message (svrId=$key)")
            return
        }
        pendingCount.incrementAndGet()
        try {
            worker.execute {
                try {
                    if (!isActive) return@execute
                    synchronized(seenKeys) {
                        if (!seenKeys.add(key)) return@execute
                        if (seenKeys.size > 4096) {
                            // 注意:不可命名为 it —— repeat{} 内 it 是循环下标(Int),会遮蔽迭代器
                            val iter = seenKeys.iterator()
                            repeat(1024) { if (iter.hasNext()) { iter.next(); iter.remove() } }
                        }
                    }
                    val payload = buildPayload(msg) ?: return@execute
                    postJson(payload, attempt = 0)
                } catch (e: Exception) {
                    WeLogger.e(TAG, "report task failed", e)
                } finally {
                    pendingCount.decrementAndGet()
                }
            }
        } catch (e: Exception) {
            pendingCount.decrementAndGet()
            WeLogger.e(TAG, "enqueue failed", e)
        }
    }

    // ------------------------------------------------------------------
    // 组装上报 JSON（后台线程执行，可安全查联系人显示名）
    // ------------------------------------------------------------------
    private fun buildPayload(m: MessageInfo): JSONObject? = runCatching {
        val now = System.currentTimeMillis()
        val type = m.type
        val conv = m.talker
        val isGroup = m.isInGroupChat
        val createTime = if (m.createTime > 0L) m.createTime else now
        val convName = runCatching { WeDatabaseApi.getDisplayName(conv) }.getOrDefault(conv)
        val sender = runCatching { m.sender }.getOrDefault("")
        val senderName = when {
            sender.isBlank() -> ""
            !isGroup -> convName
            else -> runCatching { WeDatabaseApi.getDisplayName(sender) }.getOrDefault(sender)
        }

        val content = m.content
        val isText = type?.isText == true
        val text = if (isText) m.actualContent else ""
        val summary = runCatching { m.humanReadableRepr }.getOrDefault("")
        val mentioned = runCatching { m.mentionedUsers }.getOrDefault(emptyList())
        val atMe = runCatching { m.isAtMe }.getOrDefault(false)
        val atAll = runCatching { m.isAnnounceAll }.getOrDefault(false)

        val msgObj = JSONObject().apply {
            put("msg_id", m.id)
            put("msg_svr_id", m.serverId)
            put("seq", m.seq)
            put("type", m.typeCode)
            put("type_name", type?.displayName ?: "unknown(${m.typeCode})")
            put("create_time", createTime)
            put("is_self", m.isSelfSender)
            put("is_send", if (m.isSelfSender) 1 else 0)
            put("conversation", conv)
            put("conversation_name", convName)
            put("is_group", isGroup)
            put("sender", sender)
            put("sender_name", senderName)
            put("text", text)
            put("summary", summary)
            put("raw_content", content.take(RAW_CONTENT_MAX))
            put("is_at_me", atMe)
            put("is_at_all", atAll)
            put("mentioned", JSONArray().apply { mentioned.forEach { put(it) } })
        }

        JSONObject().apply {
            put("event", "message")
            put("report_ts_ms", now)
            put("self_wxid", runCatching { WeApi.selfWxId }.getOrDefault(""))
            put("module", runCatching { BuildConfig.TAG }.getOrDefault("WeKite"))
            put("module_version", runCatching { BuildConfig.VERSION_NAME }.getOrDefault(""))
            put("msg", msgObj)
        }
    }.getOrElse { e ->
        WeLogger.e(TAG, "buildPayload failed", e)
        null
    }

    // ------------------------------------------------------------------
    // HTTP POST（OkHttp 异步，不阻塞调用线程）
    // ------------------------------------------------------------------
    private fun postJson(payload: JSONObject, attempt: Int) {
        val url = reportUrl
        if (url.isBlank()) return
        val token = reportToken.trim()
        try {
            val body = payload.toString().toRequestBody(jsonMediaType)
            val builder = Request.Builder().url(url).post(body)
            if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
            val request = builder.build()
            httpClient.newCall(request).enqueue(object : okhttp3.Callback {
                override fun onFailure(call: okhttp3.Call, e: java.io.IOException) {
                    WeLogger.w(TAG, "POST failed (attempt=${attempt + 1}): ${e.message}")
                    if (attempt == 0) retryLater(payload)
                }

                override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                    response.use { resp ->
                        if (!resp.isSuccessful) {
                            WeLogger.w(TAG, "POST http=${resp.code} (attempt=${attempt + 1})")
                            if (attempt == 0) retryLater(payload)
                        } else {
                            val msgObj = payload.optJSONObject("msg")
                            WeLogger.i(
                                TAG,
                                "reported ok: conv=${msgObj?.optString("conversation")} " +
                                    "type=${msgObj?.opt("type")} svr=${msgObj?.optLong("msg_svr_id")}"
                            )
                        }
                    }
                }
            })
        } catch (e: Exception) {
            WeLogger.e(TAG, "POST error", e)
        }
    }

    private fun retryLater(payload: JSONObject) {
        val w = worker ?: return
        try {
            w.execute { postJson(payload, attempt = 1) }
        } catch (e: Exception) {
            WeLogger.e(TAG, "retry enqueue failed", e)
        }
    }

    // ------------------------------------------------------------------
    // 设置对话框（点击功能行打开）
    // ------------------------------------------------------------------
    override fun onClick(context: ComponentActivity) {
        showComposeDialog(context) {
            var urlInput by remember { mutableStateOf(reportUrl) }
            var modeInput by remember { mutableStateOf(reportMode) }
            var includeSelfInput by remember { mutableStateOf(includeSelf) }
            var onlyTextInput by remember { mutableStateOf(onlyText) }
            var tokenInput by remember { mutableStateOf(reportToken) }

            AlertDialogContent(
                title = { Text("消息实时上报") },
                text = {
                    DefaultColumn(Modifier.verticalScroll(rememberScrollState())) {
                        Text(
                            "上报地址（POST JSON）",
                            modifier = Modifier.padding(top = 4.dp)
                        )
                        OutlinedTextField(
                            value = urlInput,
                            onValueChange = { urlInput = it.trim() },
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 4.dp),
                            label = { Text("http(s)://.../api/wechat/msg") },
                            supportingText = { Text("保存即发送一条测试消息验证链路") },
                            singleLine = true
                        )

                        Text(
                            "监听范围",
                            modifier = Modifier.padding(top = 12.dp)
                        )
                        SingleChoiceSegmentedButtonRow(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 4.dp)
                        ) {
                            listOf(
                                MODE_ALL to "全部",
                                MODE_CONTACT to "仅联系人",
                                MODE_GROUP to "仅群聊"
                            ).forEachIndexed { index, (mode, label) ->
                                SegmentedButton(
                                    selected = modeInput == mode,
                                    onClick = { modeInput = mode },
                                    shape = SegmentedButtonDefaults.itemShape(index, 3)
                                ) { Text(label) }
                            }
                        }

                        ListItem(
                            colors = dialogListItemColors(),
                            headlineContent = { Text("包含自己发出的消息") },
                            supportingContent = { Text("默认关闭，只上报收到的新消息") },
                            trailingContent = {
                                Switch(checked = includeSelfInput, onCheckedChange = { includeSelfInput = it })
                            }
                        )
                        ListItem(
                            colors = dialogListItemColors(),
                            headlineContent = { Text("仅上报文字消息") },
                            supportingContent = { Text("默认关闭，上报全部消息类型") },
                            trailingContent = {
                                Switch(checked = onlyTextInput, onCheckedChange = { onlyTextInput = it })
                            }
                        )

                        Text(
                            "鉴权 Token（可选，留空不发送）",
                            modifier = Modifier.padding(top = 12.dp)
                        )
                        OutlinedTextField(
                            value = tokenInput,
                            onValueChange = { tokenInput = it },
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 4.dp),
                            label = { Text("Authorization: Bearer <token>") },
                            singleLine = true
                        )
                    }
                },
                confirmButton = {
                    Button(onClick = {
                        reportUrl = urlInput.trim()
                        reportMode = modeInput
                        includeSelf = includeSelfInput
                        onlyText = onlyTextInput
                        reportToken = tokenInput.trim()
                        showToast("已保存，正在发送测试消息")
                        val sample = runCatching { buildTestPayload() }.getOrNull()
                        if (sample != null) postJson(sample, attempt = 0)
                        onDismiss()
                    }) { Text("保存并发送测试") }
                },
                dismissButton = { TextButton(onDismiss) { Text("取消") } }
            )
        }
    }

    /** 生成一条用于链路测试的示例消息。 */
    private fun buildTestPayload(): JSONObject? = runCatching {
        val now = System.currentTimeMillis()
        val msgObj = JSONObject().apply {
            put("msg_id", 0L)
            put("msg_svr_id", 0L)
            put("seq", 0L)
            put("type", 1)
            put("type_name", "文本")
            put("create_time", now)
            put("is_self", false)
            put("is_send", 0)
            put("conversation", "test@chatroom")
            put("conversation_name", "测试群")
            put("is_group", true)
            put("sender", "wxid_test_member")
            put("sender_name", "测试成员")
            put("text", "这是一条来自 WeKite 消息实时上报的测试消息")
            put("summary", "这是一条来自 WeKite 消息实时上报的测试消息")
            put("raw_content", "wxid_test_member:\n这是一条来自 WeKite 消息实时上报的测试消息")
            put("is_at_me", false)
            put("is_at_all", false)
            put("mentioned", JSONArray())
        }
        JSONObject().apply {
            put("event", "message")
            put("report_ts_ms", now)
            put("self_wxid", runCatching { WeApi.selfWxId }.getOrDefault(""))
            put("module", runCatching { BuildConfig.TAG }.getOrDefault("WeKite"))
            put("module_version", runCatching { BuildConfig.VERSION_NAME }.getOrDefault(""))
            put("msg", msgObj)
        }
    }.getOrElse { e ->
        WeLogger.e(TAG, "buildTestPayload failed", e)
        null
    }
}
