<template>
  <div :class="['flex h-screen w-screen overflow-hidden', isDark ? 'dark' : '']">
    <div class="flex h-full w-full bg-slate-50 text-slate-900 dark:bg-slate-950 dark:text-slate-100">
      
      <!-- 1. 左侧 Sidebar 导航栏 -->
      <aside class="w-64 flex-shrink-0 bg-white border-r border-slate-200 dark:bg-slate-900 dark:border-slate-800 flex flex-col justify-between">
        <div>
          <!-- Logo 品牌 -->
          <div class="h-16 flex items-center gap-3 px-6 border-b border-slate-100 dark:border-slate-800/60">
            <div class="h-9 w-9 rounded-xl bg-indigo-600 flex items-center justify-center text-white font-bold shadow-md shadow-indigo-500/20">
              <Download class="w-5 h-5" />
            </div>
            <div>
              <h1 class="font-bold text-base tracking-tight leading-none text-slate-800 dark:text-white">BillBill DL</h1>
              <span class="text-[11px] font-medium text-slate-400 dark:text-slate-500">Rust + Web 极速重构版</span>
            </div>
          </div>

          <!-- 导航菜单 -->
          <nav class="p-3 space-y-1">
            <button
              v-for="item in navItems"
              :key="item.id"
              @click="currentTab = item.id"
              :class="[
                'w-full flex items-center justify-between px-3.5 py-2.5 rounded-xl font-medium text-sm transition-all duration-150',
                currentTab === item.id
                  ? 'bg-indigo-50 text-indigo-600 dark:bg-indigo-950/50 dark:text-indigo-400 font-semibold'
                  : 'text-slate-600 hover:bg-slate-100/80 dark:text-slate-400 dark:hover:bg-slate-800/60'
              ]"
            >
              <div class="flex items-center gap-3">
                <component :is="item.icon" class="w-4 h-4" />
                <span>{{ item.label }}</span>
              </div>
              <span
                v-if="item.id === 'tasks' && activeTaskCount > 0"
                class="px-2 py-0.5 text-[11px] rounded-full bg-indigo-600 text-white font-bold"
              >
                {{ activeTaskCount }}
              </span>
            </button>
          </nav>
        </div>

        <!-- 底部主题与环境状态 -->
        <div class="p-4 border-t border-slate-100 dark:border-slate-800/60 space-y-3">
          <div class="flex items-center justify-between px-2 text-xs text-slate-500">
            <span class="flex items-center gap-1.5">
              <span
                :class="[
                  'w-2 h-2 rounded-full',
                  engineStatus === 'ok' ? 'bg-emerald-500 animate-pulse' :
                  engineStatus === 'degraded' ? 'bg-amber-500' : 'bg-slate-400'
                ]"
              ></span>
              <span v-if="engineStatus === 'ok'">Rust 核心引擎就绪</span>
              <span v-else-if="engineStatus === 'degraded'">引擎降级运行</span>
              <span v-else>引擎检测中…</span>
            </span>
            <span class="font-mono text-[10px]">v0.1.0</span>
          </div>

          <button
            @click="isDark = !isDark"
            class="w-full flex items-center justify-center gap-2 py-2 rounded-xl text-xs font-medium text-slate-600 bg-slate-100 hover:bg-slate-200 dark:text-slate-300 dark:bg-slate-800 dark:hover:bg-slate-700/80 transition-colors"
          >
            <Sun v-if="isDark" class="w-3.5 h-3.5" />
            <Moon v-else class="w-3.5 h-3.5" />
            <span>{{ isDark ? '浅色模式' : '深色模式' }}</span>
          </button>
        </div>
      </aside>

      <!-- 2. 主内容区域 Main Content -->
      <main class="flex-1 flex flex-col h-full overflow-hidden bg-slate-50 dark:bg-slate-950">
        <!-- WS 断线横幅 -->
        <div
          v-if="wsStatus !== 'open'"
          class="px-4 py-2 text-xs font-medium bg-amber-50 text-amber-800 border-b border-amber-200 dark:bg-amber-950/40 dark:text-amber-200 dark:border-amber-900 flex items-center justify-between"
        >
          <span>
            {{ wsStatus === 'connecting' ? '正在连接实时进度…' : '实时进度连接已断开，正在重连…' }}
          </span>
          <button @click="initWebSocket" class="underline">立即重连</button>
        </div>
        
        <!-- 页面一：下载中心（单链接 + 抖音批量） -->
        <section v-if="currentTab === 'home'" class="flex-1 overflow-y-auto p-8 max-w-5xl mx-auto w-full space-y-6">
          <div class="space-y-1">
            <h2 class="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">下载中心</h2>
            <p class="text-sm text-slate-500 dark:text-slate-400">支持抖音、Bilibili、YouTube 等；抖音主页/合集可批量抓取</p>
          </div>

          <!-- 模式切换 -->
          <div class="inline-flex rounded-xl bg-slate-100 dark:bg-slate-800 p-1">
            <button
              v-for="m in homeModes"
              :key="m.id"
              @click="homeMode = m.id"
              :class="[
                'px-4 py-2 rounded-lg text-xs font-semibold transition',
                homeMode === m.id
                  ? 'bg-white dark:bg-slate-900 text-indigo-600 dark:text-indigo-400 shadow-sm'
                  : 'text-slate-500 hover:text-slate-800 dark:hover:text-slate-200'
              ]"
            >
              {{ m.label }}
            </button>
          </div>

          <!-- 输出目录（两种模式共用） -->
          <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-4 shadow-sm">
            <div class="flex flex-col md:flex-row md:items-center gap-3">
              <label class="text-xs font-medium text-slate-500 md:w-24 flex-shrink-0">保存到</label>
              <input
                v-model="homeOutputDir"
                type="text"
                placeholder="downloads 或 D:\Videos\dl"
                class="flex-1 rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
              <button
                @click="pickOutputFolder"
                :disabled="isPickingFolder"
                class="flex items-center gap-1.5 px-4 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold disabled:opacity-50 transition"
              >
                <Loader2 v-if="isPickingFolder" class="w-3.5 h-3.5 animate-spin" />
                <FolderDown v-else class="w-3.5 h-3.5" />
                <span>{{ isPickingFolder ? '等待选择…' : '选择目录' }}</span>
              </button>
              <button
                @click="persistHomeOutputDir"
                class="px-4 py-2 rounded-xl bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-xs font-semibold text-slate-700 dark:text-slate-200 transition"
              >
                保存
              </button>
            </div>
            <p class="mt-2 text-[11px] text-slate-400">点「选择目录」会弹出系统文件夹窗口；也可手动粘贴路径</p>
          </div>

          <!-- 模式 A：单/多链接解析 -->
          <template v-if="homeMode === 'single'">
            <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-sm space-y-4">
              <textarea
                v-model="inputUrl"
                rows="3"
                placeholder="粘贴视频分享链接（抖音口令/主页带 modal_id、B站 BV 号、YouTube 等，可多行批量）..."
                class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 p-3.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-500 transition"
              ></textarea>

              <div class="flex items-center justify-between">
                <label class="flex items-center gap-1.5 cursor-pointer text-xs text-slate-500">
                  <input type="checkbox" v-model="audioOnlyOption" class="rounded text-indigo-600 focus:ring-indigo-500" />
                  <span>仅提取音频 (MP3)</span>
                </label>
                <div class="flex items-center gap-3">
                  <button
                    @click="clearHomeInput"
                    class="px-4 py-2 text-xs font-medium text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white"
                  >
                    清空
                  </button>
                  <button
                    @click="handleResolveUrl"
                    :disabled="isResolving || !inputUrl.trim()"
                    class="flex items-center gap-2 px-5 py-2.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold shadow-md shadow-indigo-500/20 disabled:opacity-50 transition"
                  >
                    <Loader2 v-if="isResolving" class="w-4 h-4 animate-spin" />
                    <Sparkles v-else class="w-4 h-4" />
                    <span>{{ isResolving ? '解析探测中...' : '开始解析' }}</span>
                  </button>
                </div>
              </div>
            </div>

            <!-- 预下载预览结果卡片 -->
            <div v-if="resolvedMeta" class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-sm space-y-4">
              <div class="flex gap-5 items-start">
                <div class="w-44 h-28 flex-shrink-0 bg-slate-100 dark:bg-slate-800 rounded-xl overflow-hidden border border-slate-200 dark:border-slate-700 relative">
                  <img v-if="resolvedMeta.cover_url" :src="resolvedMeta.cover_url" class="w-full h-full object-cover" />
                  <div v-else class="w-full h-full flex items-center justify-center text-slate-400 text-xs">无封面</div>
                  <span class="absolute bottom-1.5 right-1.5 px-1.5 py-0.5 rounded bg-black/70 text-[10px] text-white font-mono">
                    {{ formatDuration(resolvedMeta.duration) }}
                  </span>
                </div>
                <div class="flex-1 min-w-0 space-y-2">
                  <div class="flex items-center gap-2">
                    <span class="px-2 py-0.5 text-[10px] rounded-md font-bold uppercase tracking-wider bg-indigo-100 text-indigo-700 dark:bg-indigo-950/70 dark:text-indigo-400">
                      {{ resolvedMeta.platform }}
                    </span>
                    <span class="text-xs text-slate-500 dark:text-slate-400">作者：{{ resolvedMeta.author || '未知' }}</span>
                  </div>
                  <h3 class="font-bold text-base text-slate-900 dark:text-white line-clamp-2">{{ resolvedMeta.title }}</h3>
                  <p class="text-xs text-slate-400 truncate">{{ resolvedMeta.url }}</p>
                  <div class="flex flex-col gap-2 pt-2">
                    <div class="flex items-center gap-3 flex-wrap">
                      <span class="text-xs text-slate-500">检测到 {{ resolvedMeta.streams?.length || 0 }} 个可用格式流</span>
                      <select
                        v-if="streamOptions.length > 0"
                        v-model="selectedFormatId"
                        class="rounded-lg bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-2 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      >
                        <option v-for="opt in streamOptions" :key="opt.format_id" :value="opt.format_id">
                          {{ opt.label }}
                        </option>
                      </select>
                      <button
                        @click="enqueueSingleTask"
                        class="ml-auto flex items-center gap-2 px-5 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-semibold shadow-md shadow-emerald-500/20 transition"
                      >
                        <Download class="w-3.5 h-3.5" />
                        <span>立即下载</span>
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </template>

          <!-- 模式 B：抖音批量 -->
          <template v-else>
            <div class="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-5 shadow-sm space-y-4">
              <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="md:col-span-1">
                  <label class="block text-xs font-medium text-slate-500 mb-1">批量类型</label>
                  <select
                    v-model="douyinBatchType"
                    class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 p-2.5 text-xs font-medium focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  >
                    <option value="posts">主页发布作品</option>
                    <option value="mix">合集视频</option>
                  </select>
                </div>
                <div class="md:col-span-2">
                  <label class="block text-xs font-medium text-slate-500 mb-1">主页/合集链接或 ID</label>
                  <input
                    v-model="douyinTargetId"
                    type="text"
                    placeholder="https://www.douyin.com/user/MS4wLj... 或 sec_user_id"
                    class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 p-2.5 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  />
                </div>
                <div class="md:col-span-1">
                  <label class="block text-xs font-medium text-slate-500 mb-1">最多条数</label>
                  <input
                    v-model.number="douyinMaxCount"
                    type="number"
                    min="1"
                    max="100"
                    class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 p-2.5 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  />
                </div>
              </div>
              <div class="flex items-center justify-between">
                <p class="text-[11px] text-slate-400">
                  {{ isBatchResolving ? '正在抓取列表，请稍候（无 Cookie 时可能失败或很慢）...' : '建议先在系统设置保存抖音 Cookie，否则批量易失败' }}
                </p>
                <button
                  @click="handleResolveDouyinBatch"
                  :disabled="isBatchResolving || !douyinTargetId.trim()"
                  class="flex items-center justify-center gap-2 px-5 py-2.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold shadow-md shadow-indigo-500/20 disabled:opacity-50 transition"
                >
                  <Loader2 v-if="isBatchResolving" class="w-3.5 h-3.5 animate-spin" />
                  <FolderDown v-else class="w-3.5 h-3.5" />
                  <span>{{ isBatchResolving ? '抓取中…' : '抓取列表' }}</span>
                </button>
              </div>
            </div>

            <div v-if="batchItems.length > 0" class="space-y-3">
              <div class="flex items-center justify-between px-1">
                <span class="text-xs font-semibold text-slate-700 dark:text-slate-300">
                  已获取到 {{ batchItems.length }} 个作品
                </span>
                <div class="flex items-center gap-3">
                  <button @click="toggleSelectAllBatch" class="text-xs text-indigo-600 hover:underline">
                    {{ isAllBatchSelected ? '取消全选' : '全选全部' }}
                  </button>
                  <button
                    @click="enqueueSelectedBatch"
                    :disabled="selectedBatchCount === 0"
                    class="px-4 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-semibold disabled:opacity-50 transition"
                  >
                    下载选中项 ({{ selectedBatchCount }})
                  </button>
                </div>
              </div>
              <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                <div
                  v-for="item in batchItems"
                  :key="item.item_id"
                  @click="item.selected = !item.selected"
                  :class="[
                    'cursor-pointer rounded-xl border p-3 flex gap-3 transition-all',
                    item.selected
                      ? 'border-indigo-500 bg-indigo-50/40 dark:bg-indigo-950/20'
                      : 'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 hover:border-slate-300'
                  ]"
                >
                  <div class="w-16 h-20 bg-slate-100 dark:bg-slate-800 rounded-lg overflow-hidden flex-shrink-0 relative">
                    <img v-if="item.cover_url" :src="item.cover_url" class="w-full h-full object-cover" />
                    <span class="absolute bottom-1 right-1 text-[9px] bg-black/70 text-white px-1 rounded font-mono">
                      {{ formatDuration(item.duration) }}
                    </span>
                  </div>
                  <div class="flex-1 min-w-0 flex flex-col justify-between">
                    <h4 class="text-xs font-medium line-clamp-2 text-slate-800 dark:text-slate-200">{{ item.title }}</h4>
                    <div class="flex items-center justify-between text-[11px] text-slate-400">
                      <span>{{ item.author }}</span>
                      <input type="checkbox" v-model="item.selected" @click.stop class="rounded text-indigo-600" />
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </template>
        </section>

        <!-- 页面三：实时任务看板 Tasks -->
        <section v-if="currentTab === 'tasks'" class="flex-1 overflow-y-auto p-8 max-w-5xl mx-auto w-full space-y-6">
          <div class="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <h2 class="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">下载任务</h2>
              <p class="text-sm text-slate-500 dark:text-slate-400">基于 Rust Tokio 异步并发执行，WebSocket 实时毫秒级进度同步</p>
            </div>

            <!-- 正在下载 / 已完成 切换 -->
            <div class="inline-flex rounded-xl bg-slate-100 dark:bg-slate-800 p-1">
              <button
                @click="taskView = 'active'"
                :class="[
                  'px-4 py-2 rounded-lg text-xs font-semibold transition flex items-center gap-1.5',
                  taskView === 'active'
                    ? 'bg-white dark:bg-slate-900 text-indigo-600 dark:text-indigo-400 shadow-sm'
                    : 'text-slate-500 hover:text-slate-800 dark:hover:text-slate-200'
                ]"
              >
                正在下载
                <span
                  v-if="activeTaskCount > 0"
                  class="px-1.5 py-0.5 text-[10px] rounded-full bg-indigo-600 text-white"
                >{{ activeTaskCount }}</span>
              </button>
              <button
                @click="taskView = 'done'"
                :class="[
                  'px-4 py-2 rounded-lg text-xs font-semibold transition flex items-center gap-1.5',
                  taskView === 'done'
                    ? 'bg-white dark:bg-slate-900 text-indigo-600 dark:text-indigo-400 shadow-sm'
                    : 'text-slate-500 hover:text-slate-800 dark:hover:text-slate-200'
                ]"
              >
                已完成
                <span
                  v-if="finishedTaskCount > 0"
                  class="px-1.5 py-0.5 text-[10px] rounded-full bg-slate-500 text-white"
                >{{ finishedTaskCount }}</span>
              </button>
            </div>
          </div>

          <div v-if="visibleTasks.length === 0" class="h-64 flex flex-col items-center justify-center border-2 border-dashed border-slate-200 dark:border-slate-800 rounded-2xl text-slate-400 space-y-2">
            <Inbox class="w-8 h-8 stroke-1" />
            <span class="text-sm">
              {{ taskView === 'active' ? '当前没有进行中的下载任务' : '暂无已完成/失败任务' }}
            </span>
          </div>

          <div v-else class="space-y-3">
            <div
              v-for="task in visibleTasks"
              :key="task.task_id"
              class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-4 shadow-sm space-y-3"
            >
              <div class="flex items-center justify-between gap-4">
                <div class="flex items-center gap-3 min-w-0 flex-1">
                  <div :class="['w-2.5 h-2.5 rounded-full flex-shrink-0', getStatusDotClass(task.status)]"></div>
                  <div class="min-w-0 flex-1">
                    <h4 class="text-sm font-semibold truncate text-slate-900 dark:text-white">
                      {{ task.title || task.task_id }}
                    </h4>
                    <div class="flex items-center gap-2 text-xs text-slate-400">
                      <span class="font-mono uppercase">{{ task.platform || '?' }}</span>
                      <span class="font-mono">{{ task.status.toUpperCase() }}</span>
                    </div>
                  </div>
                </div>

                <div class="flex items-center gap-2 text-xs font-mono">
                  <span v-if="task.status === 'downloading'" class="text-indigo-600 dark:text-indigo-400 font-bold">
                    {{ formatSpeed(task.speed_bps) }}
                  </span>
                  <button
                    v-if="['downloading', 'resolving', 'merging', 'pending'].includes(task.status)"
                    @click="cancelTask(task.task_id)"
                    class="px-2.5 py-1 text-xs rounded-lg text-red-600 hover:bg-red-50 dark:hover:bg-red-950/40"
                  >
                    取消
                  </button>
                  <button
                    v-if="task.can_retry && ['failed', 'cancelled', 'completed'].includes(task.status)"
                    @click="retryTask(task.task_id)"
                    class="px-2.5 py-1 text-xs rounded-lg text-indigo-600 hover:bg-indigo-50 dark:hover:bg-indigo-950/40 font-sans"
                  >
                    重试
                  </button>
                </div>
              </div>

              <p
                v-if="task.error_msg"
                class="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-950/30 rounded-lg px-3 py-2"
              >
                {{ task.error_msg }}
                <button
                  v-if="isAuthError(task.error_msg)"
                  @click="currentTab = 'settings'"
                  class="ml-2 underline font-semibold"
                >
                  去设置更新 Cookie
                </button>
              </p>

              <p v-if="task.output_path && task.status === 'completed'" class="text-[11px] text-slate-400 truncate font-mono">
                {{ task.output_path }}
              </p>

              <!-- 进度条：进行中始终显示；已完成折叠为满条 -->
              <div class="space-y-1">
                <div class="w-full bg-slate-100 dark:bg-slate-800 h-2 rounded-full overflow-hidden">
                  <div
                    :class="[
                      'h-full rounded-full transition-all duration-300',
                      task.status === 'completed' ? 'bg-emerald-500' : 'bg-indigo-600'
                    ]"
                    :style="{ width: `${task.percentage || (task.status === 'completed' ? 100 : 0)}%` }"
                  ></div>
                </div>
                <div class="flex items-center justify-between text-[11px] text-slate-400 font-mono">
                  <span>{{ formatBytes(task.downloaded_bytes) }} / {{ formatBytes(task.total_bytes) }}</span>
                  <span>{{ (task.percentage || (task.status === 'completed' ? 100 : 0)).toFixed(1) }}%</span>
                </div>
              </div>
            </div>
          </div>
        </section>

        <!-- 页面四：历史记录 History -->
        <section v-if="currentTab === 'history'" class="flex-1 overflow-y-auto p-8 max-w-5xl mx-auto w-full space-y-6">
          <div class="flex items-center justify-between">
            <div>
              <h2 class="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">下载历史</h2>
              <p class="text-sm text-slate-500 dark:text-slate-400">本地 SQLite 存储，已启用全文检索 (FTS5)</p>
            </div>
            <div class="w-64">
              <input
                v-model="historySearchQuery"
                @input="handleSearchHistory"
                type="text"
                placeholder="搜索标题、作者或链接..."
                class="w-full rounded-xl bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 px-3.5 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
            </div>
          </div>

          <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl overflow-hidden shadow-sm">
            <table class="w-full text-left text-xs">
              <thead class="bg-slate-50 dark:bg-slate-800/60 text-slate-500 font-semibold border-b border-slate-200 dark:border-slate-800">
                <tr>
                  <th class="p-3.5">标题</th>
                  <th class="p-3.5">平台</th>
                  <th class="p-3.5">作者</th>
                  <th class="p-3.5">大小</th>
                  <th class="p-3.5">时间</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono">
                <tr v-for="rec in historyRecords" :key="rec.id" class="hover:bg-slate-50/60 dark:hover:bg-slate-800/40">
                  <td class="p-3.5 font-sans font-medium text-slate-900 dark:text-white truncate max-w-xs">{{ rec.title }}</td>
                  <td class="p-3.5 uppercase text-indigo-600">{{ rec.platform }}</td>
                  <td class="p-3.5 font-sans">{{ rec.author || '-' }}</td>
                  <td class="p-3.5">{{ formatBytes(rec.file_size) }}</td>
                  <td class="p-3.5 text-slate-400">{{ rec.timestamp }}</td>
                </tr>
                <tr v-if="historyRecords.length === 0">
                  <td colspan="5" class="p-8 text-center text-slate-400 font-sans">暂无历史记录</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <!-- 页面五：设置中心 Settings -->
        <section v-if="currentTab === 'settings'" class="flex-1 overflow-y-auto p-8 max-w-4xl mx-auto w-full space-y-6">
          <div class="space-y-1">
            <h2 class="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">系统设置与诊断</h2>
            <p class="text-sm text-slate-500 dark:text-slate-400">Cookie 健康度诊断、网络代理配置及运行环境自愈体检</p>
          </div>

          <!-- Cookie 诊断卡片 -->
          <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-5 shadow-sm space-y-4">
            <h3 class="font-bold text-sm text-slate-900 dark:text-white flex items-center gap-2">
              <ShieldCheck class="w-4 h-4 text-indigo-600" />
              <span>Cookie 录入与健康度体检</span>
            </h3>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
              <div class="md:col-span-1">
                <label class="block text-xs font-medium text-slate-500 mb-1">平台选择</label>
                <select
                  v-model="cookiePlatform"
                  class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 p-2.5 text-xs font-medium focus:outline-none focus:ring-2 focus:ring-indigo-500"
                >
                  <option value="douyin">抖音 (Douyin)</option>
                  <option value="bilibili">哔哩哔哩 (Bilibili)</option>
                </select>
              </div>

              <div class="md:col-span-3">
                <label class="block text-xs font-medium text-slate-500 mb-1">Cookie 字符串</label>
                <textarea
                  v-model="cookieInput"
                  rows="2"
                  placeholder="粘贴从浏览器 DevTools 复制的 Cookie 字符串..."
                  class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 p-2.5 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-indigo-500"
                ></textarea>
              </div>
            </div>

            <div class="flex items-center justify-between">
              <div class="flex items-center gap-2">
                <button
                  @click="checkCookieHealth"
                  :disabled="!cookieInput.trim()"
                  class="px-4 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold disabled:opacity-50 transition"
                >
                  立即健康诊断
                </button>
                <button
                  @click="saveCookieLocal"
                  class="px-4 py-2 rounded-xl bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 text-xs font-semibold transition"
                >
                  保存到本机
                </button>
                <button
                  v-if="cookieInput"
                  @click="clearCookieLocal"
                  class="px-3 py-2 rounded-xl text-xs text-red-600 hover:bg-red-50 dark:hover:bg-red-950/40 transition"
                >
                  清除
                </button>
              </div>
              <p class="text-[11px] text-slate-400">仅存于本机浏览器 localStorage，不会上传</p>
            </div>

            <!-- 诊断结果展示 -->
            <div v-if="cookieDiagnosis" :class="['p-3.5 rounded-xl border text-xs space-y-1.5', cookieDiagnosis.valid ? 'bg-emerald-50 border-emerald-200 dark:bg-emerald-950/20 text-emerald-800 dark:text-emerald-300' : 'bg-amber-50 border-amber-200 dark:bg-amber-950/20 text-amber-800 dark:text-amber-300']">
              <div class="font-bold flex items-center gap-1.5">
                <span>{{ cookieDiagnosis.valid ? '✓ Cookie 健康状态良好' : '⚠ Cookie 存在缺失或失效项' }}</span>
              </div>
              <div v-if="cookieDiagnosis.missing?.length" class="text-[11px]">
                <span class="font-semibold">缺失必需项：</span>{{ cookieDiagnosis.missing.join(', ') }}
              </div>
              <div v-if="cookieDiagnosis.warnings?.length" class="text-[11px]">
                <span class="font-semibold">警告提醒：</span>{{ cookieDiagnosis.warnings.join('；') }}
              </div>
            </div>
          </div>

          <!-- 移动端 / 远程服务地址配置 -->
          <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-5 shadow-sm space-y-3">
            <div class="flex items-center justify-between">
              <div>
                <h3 class="font-bold text-sm text-slate-900 dark:text-white">后端服务地址 (Server Host)</h3>
                <p class="text-xs text-slate-500">用于手机 App (APK) 连回家中电脑。若在电脑浏览器内使用，请保持留空。</p>
              </div>
              <button
                @click="saveServerHost"
                class="px-3 py-1.5 rounded-xl text-xs font-semibold bg-indigo-600 hover:bg-indigo-700 text-white transition-colors"
              >
                保存并重连
              </button>
            </div>
            <div>
              <input
                v-model="serverHost"
                type="text"
                placeholder="例如：http://192.168.124.5:18080（留空为默认同源请求）"
                class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono"
              />
            </div>
          </div>

          <!-- 下载偏好设置 -->
          <div class="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-5 shadow-sm space-y-4">
            <h3 class="font-bold text-sm text-slate-900 dark:text-white">下载偏好（保存到本机数据库）</h3>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label class="block text-xs font-medium text-slate-500 mb-1">默认输出目录（相对项目根或绝对路径）</label>
                <input
                  v-model="appSettings.output_dir"
                  type="text"
                  placeholder="downloads 或 D:\Videos"
                  class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label class="block text-xs font-medium text-slate-500 mb-1">默认代理（可选，如 socks5://127.0.0.1:7890）</label>
                <input
                  v-model="appSettings.proxy"
                  type="text"
                  placeholder="留空则不使用代理"
                  class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label class="block text-xs font-medium text-slate-500 mb-1">限速（KB/s，0 = 不限）</label>
                <input
                  v-model.number="appSettings.rate_limit_kbps"
                  type="number"
                  min="0"
                  class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label class="block text-xs font-medium text-slate-500 mb-1">最大并发任务数（1-32）</label>
                <input
                  v-model.number="appSettings.max_concurrent"
                  type="number"
                  min="1"
                  max="32"
                  class="w-full rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800 px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <label class="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300">
                <input type="checkbox" v-model="appSettings.download_cover" class="rounded text-indigo-600" />
                同时下载封面图
              </label>
              <label class="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300">
                <input type="checkbox" v-model="appSettings.download_subs" class="rounded text-indigo-600" />
                同时下载字幕（如有）
              </label>
            </div>
            <div class="flex justify-end">
              <button
                @click="saveAppSettings"
                class="px-4 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold transition"
              >
                保存设置
              </button>
            </div>
          </div>
        </section>

      </main>
    </div>

    <!-- Toast 通知 -->
    <div class="fixed top-4 right-4 z-50 space-y-2 pointer-events-none">
      <div
        v-for="t in toasts"
        :key="t.id"
        :class="[
          'pointer-events-auto max-w-sm px-4 py-3 rounded-xl shadow-lg text-xs font-medium border backdrop-blur',
          t.type === 'error' ? 'bg-red-50/95 border-red-200 text-red-800 dark:bg-red-950/90 dark:text-red-200 dark:border-red-800' :
          t.type === 'success' ? 'bg-emerald-50/95 border-emerald-200 text-emerald-800 dark:bg-emerald-950/90 dark:text-emerald-200 dark:border-emerald-800' :
          'bg-slate-50/95 border-slate-200 text-slate-800 dark:bg-slate-800/95 dark:text-slate-100 dark:border-slate-700'
        ]"
      >
        {{ t.message }}
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, watch } from 'vue'
import {
  Download,
  FolderDown,
  Sparkles,
  Inbox,
  History,
  Settings,
  Sun,
  Moon,
  Loader2,
  ShieldCheck,
} from 'lucide-vue-next'

// 主题状态
const isDark = ref(false)

// 引擎健康状态：unknown | ok | degraded
const engineStatus = ref('unknown')
const wsStatus = ref('connecting') // connecting | open | closed

// Toast
const toasts = ref([])
let toastSeq = 0
const toast = (message, type = 'info') => {
  const id = ++toastSeq
  toasts.value.push({ id, message, type })
  setTimeout(() => {
    toasts.value = toasts.value.filter(t => t.id !== id)
  }, 4000)
}

const COOKIE_STORAGE_KEY = 'ddl.cookie'

// App settings (server-persisted)
const defaultSettings = {
  output_dir: 'downloads',
  rate_limit_kbps: 0,
  max_concurrent: 5,
  proxy: '',
  download_cover: false,
  download_subs: false,
}
const appSettings = ref({ ...defaultSettings })

// 后端服务器地址配置（用于移动端 App / APK 连接 PC 后端）
const serverHost = ref(localStorage.getItem('ddl_server_host') || '')

const getApiBase = () => {
  if (serverHost.value && serverHost.value.trim()) {
    return serverHost.value.trim().replace(/\/+$/, '')
  }
  return ''
}

const saveServerHost = () => {
  if (serverHost.value && serverHost.value.trim()) {
    localStorage.setItem('ddl_server_host', serverHost.value.trim().replace(/\/+$/, ''))
    toast('后端服务器地址已保存，正在重新连接…', 'success')
  } else {
    localStorage.removeItem('ddl_server_host')
    toast('已切换为默认相对路径模式', 'info')
  }
  initWebSocket()
  loadAppSettings()
  checkEngineHealth()
  loadTasks()
}

const apiFetch = (url, options) => {
  const base = getApiBase()
  const full = base ? (base + (url.startsWith('/') ? url : '/' + url)) : url
  return fetch(full, options)
}

const loadAppSettings = async () => {
  try {
    const res = await apiFetch('/api/settings')
    if (res.ok) {
      appSettings.value = { ...defaultSettings, ...(await res.json()) }
      homeOutputDir.value = appSettings.value.output_dir || 'downloads'
    }
  } catch (e) {}
}

const saveAppSettings = async () => {
  try {
    const res = await apiFetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(appSettings.value),
    })
    if (!res.ok) throw new Error(await res.text())
    appSettings.value = await res.json()
    toast('设置已保存', 'success')
  } catch (err) {
    toast('保存设置失败: ' + err.message, 'error')
  }
}

// 导航菜单定义
const currentTab = ref('home')
const navItems = [
  { id: 'home', label: '下载中心', icon: Download },
  { id: 'tasks', label: '进行任务', icon: Inbox },
  { id: 'history', label: '历史归档', icon: History },
  { id: 'settings', label: '系统设置', icon: Settings },
]

// 下载中心内部模式
const homeMode = ref('single')
const homeModes = [
  { id: 'single', label: '单链接解析' },
  { id: 'batch', label: '抖音批量' },
]
const homeOutputDir = ref('downloads')
const isPickingFolder = ref(false)
const douyinMaxCount = ref(20)

// 1. 下载中心状态
const inputUrl = ref('')
const isResolving = ref(false)
const resolvedMeta = ref(null)
const audioOnlyOption = ref(false)
const selectedFormatId = ref('')

const clearHomeInput = () => {
  inputUrl.value = ''
  resolvedMeta.value = null
  selectedFormatId.value = ''
}

const streamOptions = computed(() => {
  const streams = resolvedMeta.value?.streams || []
  return streams.map((s, idx) => {
    const kind = s.video_url && !s.audio_url ? '视频' : s.audio_url && !s.video_url ? '音频' : '合流'
    const res = s.resolution || '-'
    const size = s.file_size_approx ? formatBytes(s.file_size_approx) : ''
    return {
      format_id: s.format_id,
      label: `${res} · ${kind}${size ? ' · ' + size : ''} · ${s.format_id}`,
      idx,
    }
  })
})

const isAuthError = (msg) => {
  if (!msg) return false
  const s = String(msg).toLowerCase()
  return s.includes('cookie') || s.includes('登录') || s.includes('auth') || s.includes('401') || s.includes('403')
}

const handleResolveUrl = async () => {
  if (!inputUrl.value.trim()) return
  isResolving.value = true
  resolvedMeta.value = null
  selectedFormatId.value = ''

  try {
    const res = await apiFetch('/api/resolve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: inputUrl.value.trim(), cookie: cookieInput.value || null }),
    })
    if (!res.ok) {
      throw new Error(await res.text())
    }
    resolvedMeta.value = await res.json()
    // Default to first stream (sidecar already prefers best quality)
    const streams = resolvedMeta.value.streams || []
    selectedFormatId.value = streams[0]?.format_id || ''
    if (isAuthError(resolvedMeta.value.error_msg)) {
      // no-op; resolve success path
    }
  } catch (err) {
    const msg = String(err.message || err)
    if (isAuthError(msg)) {
      if (confirm('解析失败，可能需要更新 Cookie。\n是否打开系统设置？')) {
        currentTab.value = 'settings'
      }
    } else {
      toast('解析失败: ' + msg, 'error')
    }
  } finally {
    isResolving.value = false
  }
}

const enqueueSingleTask = async () => {
  if (!resolvedMeta.value) return
  try {
    const res = await apiFetch('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        url: resolvedMeta.value.url,
        extract_audio: audioOnlyOption.value ? 'mp3' : null,
        cookie: cookieInput.value || null,
        format_id: selectedFormatId.value || null,
        output_dir: homeOutputDir.value || null,
      }),
    })
    if (!res.ok) throw new Error(await res.text())
    toast('任务已创建', 'success')
    currentTab.value = 'tasks'
    loadTasks()
  } catch (err) {
    toast('创建任务失败: ' + err.message, 'error')
  }
}

const pickOutputFolder = async () => {
  if (isPickingFolder.value) return
  isPickingFolder.value = true
  toast('请在弹出的系统窗口中选择文件夹…', 'info')
  try {
    const res = await apiFetch('/api/dialog/pick-folder', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ default: homeOutputDir.value || 'downloads' }),
    })
    if (!res.ok) throw new Error(await res.text())
    const data = await res.json()
    if (data.cancelled || !data.path) {
      toast('已取消选择', 'info')
      return
    }
    homeOutputDir.value = data.path
    // Auto-save after pick
    await persistHomeOutputDir()
  } catch (err) {
    toast('选择目录失败: ' + (err.message || err), 'error')
  } finally {
    isPickingFolder.value = false
  }
}

const persistHomeOutputDir = async () => {
  try {
    const next = { ...appSettings.value, output_dir: homeOutputDir.value || 'downloads' }
    const res = await apiFetch('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(next),
    })
    if (!res.ok) throw new Error(await res.text())
    appSettings.value = await res.json()
    homeOutputDir.value = appSettings.value.output_dir
    toast('下载目录已保存', 'success')
  } catch (err) {
    toast('保存目录失败: ' + err.message, 'error')
  }
}

// 2. 抖音批量状态
const douyinBatchType = ref('posts')
const douyinTargetId = ref('')
const isBatchResolving = ref(false)
const batchItems = ref([])

const handleResolveDouyinBatch = async () => {
  if (!douyinTargetId.value.trim()) {
    toast('请先填写主页链接或 ID', 'error')
    return
  }
  if (!cookieInput.value || !cookieInput.value.trim()) {
    toast('批量抓取需要 Cookie：请先到「系统设置」粘贴并保存，再回来重试', 'error')
    currentTab.value = 'settings'
    return
  }
  isBatchResolving.value = true
  batchItems.value = []

  let cleanId = douyinTargetId.value.trim()
  // Extract from various URL shapes
  const userMatch = cleanId.match(/user\/([a-zA-Z0-9_-]+)/)
  if (userMatch) {
    cleanId = userMatch[1]
    if (douyinBatchType.value === 'mix') {
      // keep as-is unless mix_id present
    }
  }
  const mixMatch = cleanId.match(/collection\/(\d+)/) || cleanId.match(/mix_id=(\d+)/)
  if (mixMatch) {
    cleanId = mixMatch[1]
    douyinBatchType.value = 'mix'
  }

  const maxCount = Math.max(1, Math.min(100, Number(douyinMaxCount.value) || 20))
  toast(`正在抓取最多 ${maxCount} 条，请稍候…`, 'info')

  try {
    const res = await apiFetch('/api/resolve/douyin/batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        batch_type: douyinBatchType.value,
        target_id: cleanId,
        max_count: maxCount,
        cookie: cookieInput.value || null,
        proxy: appSettings.value.proxy || null,
      }),
    })
    if (!res.ok) throw new Error(await res.text())
    const data = await res.json()
    const items = (data.sub_items || []).map(item => ({ ...item, selected: true }))
    batchItems.value = items
    if (items.length === 0) {
      toast('未抓到作品：可能需要登录 Cookie，或该主页无公开作品', 'error')
    } else {
      toast(`抓取成功，共 ${items.length} 条`, 'success')
    }
  } catch (err) {
    toast('抓取批量失败: ' + (err.message || err), 'error')
  } finally {
    isBatchResolving.value = false
  }
}

const selectedBatchCount = computed(() => batchItems.value.filter(i => i.selected).length)
const isAllBatchSelected = computed(() => batchItems.value.length > 0 && selectedBatchCount.value === batchItems.value.length)

const toggleSelectAllBatch = () => {
  const target = !isAllBatchSelected.value
  batchItems.value.forEach(i => (i.selected = target))
}

const enqueueSelectedBatch = async () => {
  const selected = batchItems.value.filter(i => i.selected)
  if (selected.length === 0) return

  // Create tasks with limited concurrency so we don't overwhelm the API.
  const CONCURRENCY = 5
  let failed = 0
  for (let i = 0; i < selected.length; i += CONCURRENCY) {
    const chunk = selected.slice(i, i + CONCURRENCY)
    const results = await Promise.all(
      chunk.map(async (item) => {
        try {
          const res = await apiFetch('/api/tasks', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              url: item.url,
              cookie: cookieInput.value || null,
              output_dir: homeOutputDir.value || null,
              title: item.title || null,
            }),
          })
          return res.ok
        } catch {
          return false
        }
      })
    )
    failed += results.filter(ok => !ok).length
  }

  if (failed > 0) {
    toast(`已提交 ${selected.length - failed} 个任务，${failed} 个创建失败`, 'error')
  } else {
    toast(`已提交 ${selected.length} 个批量任务`, 'success')
  }
  currentTab.value = 'tasks'
  loadTasks()
}

// 3. 任务与 WebSocket 实时推送
const tasks = ref([])
/** active | done */
const taskView = ref('active')

const ACTIVE_STATUSES = ['resolving', 'downloading', 'merging', 'pending']
const isTaskActive = (t) => ACTIVE_STATUSES.includes(t?.status)

const activeTasks = computed(() => tasks.value.filter(isTaskActive))
const finishedTasks = computed(() => tasks.value.filter((t) => !isTaskActive(t)))
const activeTaskCount = computed(() => activeTasks.value.length)
const finishedTaskCount = computed(() => finishedTasks.value.length)
const visibleTasks = computed(() =>
  taskView.value === 'active' ? activeTasks.value : finishedTasks.value
)

const loadTasks = async () => {
  try {
    const res = await apiFetch('/api/tasks')
    if (res.ok) {
      tasks.value = await res.json()
    }
  } catch (e) {}
}

const cancelTask = async (taskId) => {
  await apiFetch(`/api/tasks/${taskId}/cancel`, { method: 'POST' })
  loadTasks()
}

const retryTask = async (taskId) => {
  try {
    const res = await apiFetch(`/api/tasks/${taskId}/retry`, { method: 'POST' })
    if (!res.ok) throw new Error(await res.text())
    toast('已重新入队', 'success')
    loadTasks()
  } catch (err) {
    toast('重试失败: ' + err.message, 'error')
  }
}

const initWebSocket = () => {
  wsStatus.value = 'connecting'
  const base = getApiBase()
  let wsUrl
  if (base) {
    const wsProto = base.startsWith('https:') ? 'wss:' : 'ws:'
    const host = base.replace(/^https?:\/\//, '')
    wsUrl = `${wsProto}//${host}/api/ws`
  } else {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    wsUrl = `${protocol}//${window.location.host}/api/ws`
  }
  let ws
  try {
    ws = new WebSocket(wsUrl)
  } catch {
    wsStatus.value = 'closed'
    setTimeout(initWebSocket, 3000)
    return
  }

  ws.onopen = () => {
    wsStatus.value = 'open'
  }

  ws.onmessage = (event) => {
    try {
      const prog = JSON.parse(event.data)
      const idx = tasks.value.findIndex(t => t.task_id === prog.task_id)
      if (idx !== -1) {
        const prev = tasks.value[idx]
        tasks.value[idx] = { ...prev, ...prog }
      } else {
        tasks.value.unshift(prog)
      }
      if (prog.status === 'completed') {
        toast(`下载完成：${prog.title || prog.task_id}`, 'success')
        loadHistory()
      } else if (prog.status === 'failed') {
        toast(`任务失败：${prog.error_msg || prog.title || prog.task_id}`, 'error')
      }
    } catch (e) {}
  }

  ws.onclose = () => {
    wsStatus.value = 'closed'
    setTimeout(initWebSocket, 3000)
  }
  ws.onerror = () => {
    wsStatus.value = 'closed'
  }
}

// 4. 历史记录状态
const historyRecords = ref([])
const historySearchQuery = ref('')

const loadHistory = async () => {
  try {
    const res = await apiFetch('/api/history')
    if (res.ok) {
      historyRecords.value = await res.json()
    }
  } catch (e) {}
}

const handleSearchHistory = async () => {
  const q = historySearchQuery.value.trim()
  if (!q) {
    loadHistory()
    return
  }
  try {
    const res = await apiFetch(`/api/history/search?q=${encodeURIComponent(q)}`)
    if (res.ok) {
      historyRecords.value = await res.json()
    }
  } catch (e) {}
}

// 5. 设置中心与 Cookie 诊断
const cookiePlatform = ref('douyin')
const cookieInput = ref('')
const cookieDiagnosis = ref(null)

const checkCookieHealth = async () => {
  try {
    const res = await apiFetch('/api/cookie/check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ platform: cookiePlatform.value, cookie: cookieInput.value }),
    })
    if (res.ok) {
      cookieDiagnosis.value = await res.json()
    }
  } catch (e) {}
}

const saveCookieLocal = () => {
  try {
    if (cookieInput.value.trim()) {
      localStorage.setItem(COOKIE_STORAGE_KEY, cookieInput.value)
      localStorage.setItem(COOKIE_STORAGE_KEY + '.platform', cookiePlatform.value)
      toast('Cookie 已保存到本机浏览器', 'success')
    } else {
      clearCookieLocal()
    }
  } catch (e) {
    toast('保存失败: ' + e.message, 'error')
  }
}

const clearCookieLocal = () => {
  try {
    localStorage.removeItem(COOKIE_STORAGE_KEY)
    localStorage.removeItem(COOKIE_STORAGE_KEY + '.platform')
    cookieInput.value = ''
    cookieDiagnosis.value = null
  } catch (e) {}
}

const loadCookieLocal = () => {
  try {
    const saved = localStorage.getItem(COOKIE_STORAGE_KEY)
    if (saved) cookieInput.value = saved
    const platform = localStorage.getItem(COOKIE_STORAGE_KEY + '.platform')
    if (platform) cookiePlatform.value = platform
  } catch (e) {}
}

const checkEngineHealth = async () => {
  try {
    const res = await apiFetch('/api/health')
    if (res.ok) {
      const data = await res.json()
      engineStatus.value = data.status === 'ok' ? 'ok' : 'degraded'
    } else {
      engineStatus.value = 'degraded'
    }
  } catch {
    engineStatus.value = 'degraded'
  }
}

// 辅助工具格式化
const formatDuration = (seconds) => {
  if (!seconds) return '00:00'
  const m = Math.floor(seconds / 60)
  const s = seconds % 60
  return `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`
}

const formatBytes = (bytes) => {
  if (!bytes || bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i]
}

const formatSpeed = (bps) => {
  if (!bps) return '0 KB/s'
  return formatBytes(bps) + '/s'
}

const getStatusDotClass = (status) => {
  switch (status) {
    case 'downloading': return 'bg-indigo-500 animate-ping'
    case 'completed': return 'bg-emerald-500'
    case 'failed': return 'bg-red-500'
    case 'cancelled': return 'bg-slate-400'
    default: return 'bg-amber-500'
  }
}

onMounted(() => {
  loadCookieLocal()
  loadAppSettings()
  checkEngineHealth()
  loadTasks()
  loadHistory()
  initWebSocket()
  setInterval(checkEngineHealth, 30_000)
})

// 切换到历史页时强制刷新
watch(currentTab, (tab) => {
  if (tab === 'history') loadHistory()
  if (tab === 'tasks') loadTasks()
})
</script>
