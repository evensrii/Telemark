(function () {
  const script = document.currentScript;
  const apiUrl = script.getAttribute("data-api-url");
  const mockMode = script.getAttribute("data-mock") === "true" || !apiUrl;

  const MOCK_ANSWER = {
    answer:
      "Dette er et demosvar fra prototypen — backend-et med Azure OpenAI er ikke koblet til ennå, så dette er ikke et ekte svar basert på innholdet.",
    sources: [{ title: "Kunnskap om Telemark (eksempelkilde)" }],
  };

  function mockChat() {
    return new Promise((resolve) => setTimeout(() => resolve(MOCK_ANSWER), 500));
  }

  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = host.attachShadow({ mode: "open" });

  root.innerHTML = `
    <style>
      .box { position: fixed; bottom: 20px; right: 20px; width: 320px; height: 420px;
             border: 1px solid #ccc; border-radius: 8px; background: #fff; display: flex;
             flex-direction: column; font-family: sans-serif; box-shadow: 0 2px 10px rgba(0,0,0,.2); }
      .header { background: #00629b; color: #fff; padding: 10px; border-radius: 8px 8px 0 0; font-weight: bold; }
      .messages { flex: 1; overflow-y: auto; padding: 10px; font-size: 14px; }
      .msg { margin-bottom: 10px; white-space: pre-wrap; }
      .msg.user { text-align: right; color: #00629b; }
      .sources { font-size: 11px; color: #888; margin-top: 4px; }
      .input-row { display: flex; border-top: 1px solid #eee; }
      input { flex: 1; border: none; padding: 10px; font-size: 14px; }
      button { border: none; background: #00629b; color: #fff; padding: 0 14px; cursor: pointer; }
    </style>
    <div class="box">
      <div class="header">Kunnskapsboten (prototype${mockMode ? " – demo, ikke ekte svar" : ""})</div>
      <div class="messages"></div>
      <div class="input-row">
        <input type="text" placeholder="Skriv et spørsmål..." />
        <button>Send</button>
      </div>
    </div>
  `;

  const messagesEl = root.querySelector(".messages");
  const inputEl = root.querySelector("input");
  const buttonEl = root.querySelector("button");

  function addMessage(text, sender) {
    const div = document.createElement("div");
    div.className = `msg ${sender}`;
    div.textContent = text;
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
    return div;
  }

  async function sendQuestion() {
    const question = inputEl.value.trim();
    if (!question) return;
    inputEl.value = "";
    addMessage(question, "user");
    const pending = addMessage("...", "bot");

    try {
      const data = mockMode
        ? await mockChat()
        : await fetch(apiUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question }),
          }).then((r) => r.json());
      pending.textContent = data.answer;

      if (data.sources && data.sources.length) {
        const sourcesEl = document.createElement("div");
        sourcesEl.className = "sources";
        sourcesEl.textContent = "Kilder: " + data.sources.map((s) => s.title).join(", ");
        pending.appendChild(sourcesEl);
      }
    } catch (err) {
      pending.textContent = "Beklager, noe gikk galt.";
    }
  }

  buttonEl.addEventListener("click", sendQuestion);
  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") sendQuestion();
  });
})();
