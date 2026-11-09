import { createLazyRuntimeModule } from "openclaw/plugin-sdk/lazy-runtime";
import { definePluginEntry, type AnyAgentTool } from "openclaw/plugin-sdk/plugin-entry";
import { canvasA2UIBoardWidgetKind } from "./src/board-widget.js";
import { canvasConfigSchema, isCanvasHostEnabled } from "./src/config.js";
import { A2UI_PATH } from "./src/host/a2ui-shared.js";
import { canvasToolDefinition } from "./src/tool-schema.js";
import { createCanvasWidgetPresenter } from "./src/widget-presenter.js";

const CANVAS_NODE_COMMANDS = ["canvas.present", "canvas.hide", "canvas.navigate"];

function createLazyCanvasTool(agentSessionKey?: string): AnyAgentTool {
  const loadTool = createLazyRuntimeModule(() =>
    import("./src/tool.js").then(({ createCanvasTool }) => createCanvasTool({ agentSessionKey })),
  );
  return {
    ...canvasToolDefinition,
    execute: async (...args: Parameters<AnyAgentTool["execute"]>) =>
      await (await loadTool()).execute(...args),
  };
}

export default definePluginEntry({
  id: "canvas",
  name: "Canvas",
  description: "Presents hosted widget documents on paired macOS panels.",
  configSchema: canvasConfigSchema,
  reload: {
    // Only canvas entry state needs a process restart (HTTP/WS host registration).
    // Allow/deny/enabled policy changes stay on the generic plugins hot-reload path;
    // claiming those prefixes forced a full gateway restart on every plugin enable.
    restartPrefixes: ["plugins.entries.canvas"],
  },
  register(api) {
    if (isCanvasHostEnabled(api.config)) {
      api.registerBoardWidgetContentKind(canvasA2UIBoardWidgetKind);
      const loadRenderer = createLazyRuntimeModule(() => import("./src/host/a2ui.js"));
      api.registerHttpRoute({
        path: A2UI_PATH,
        auth: "plugin",
        match: "prefix",
        nodeCapability: { surface: "canvas" },
        handler: async (req, res) => await (await loadRenderer()).handleA2uiHttpRequest(req, res),
      });
      api.registerWidgetPresenter(createCanvasWidgetPresenter(api.runtime.nodes));
    }
    api.registerNodeInvokePolicy({
      commands: CANVAS_NODE_COMMANDS,
      defaultPlatforms: ["macos"],
      handle: async (ctx) => await ctx.invokeNode(),
    });
    api.registerTool((ctx) => createLazyCanvasTool(ctx.sessionKey));
    api.registerNodeCliFeature(
      async ({ program }) => {
        const { registerNodesCanvasCommands } = await import("./src/cli.js");
        registerNodesCanvasCommands(program);
      },
      {
        descriptors: [
          {
            name: "canvas",
            description: "Present widget documents on a paired macOS panel",
            hasSubcommands: true,
          },
        ],
      },
    );
  },
});
