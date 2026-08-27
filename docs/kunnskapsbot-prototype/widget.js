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
      * { box-sizing: border-box; }
      .box { position: fixed; bottom: 20px; right: 20px; width: 340px; height: 480px;
             border: 1px solid #e3e0ea; border-radius: 14px; background: #fff; display: flex;
             flex-direction: column; overflow: hidden; font-family: -apple-system, "Segoe UI", Roboto, sans-serif;
             box-shadow: 0 4px 20px rgba(0,0,0,.15); transition: height .2s ease; }
      .box.collapsed { height: 56px; }
      .header { flex-shrink: 0; background: #d8cbe8; color: #1a1a2e; padding: 14px 16px;
                display: flex; align-items: center; gap: 8px; cursor: pointer; }
      .header .icon { font-size: 16px; }
      .header .title { flex: 1; font-weight: 700; font-size: 15px; }
      .header .badge { font-size: 9px; font-weight: 700; letter-spacing: .03em; color: #6b4fa0;
                        background: #fff; padding: 2px 6px; border-radius: 8px; }
      .header .chevron { font-size: 14px; transition: transform .2s ease; }
      .box.collapsed .chevron { transform: rotate(-90deg); }
      .body { flex: 1; display: flex; flex-direction: column; justify-content: center;
              padding: 20px; overflow-y: auto; }
      .body.has-messages { justify-content: flex-start; }
      .welcome { font-weight: 700; font-size: 19px; color: #0d1b4c; text-align: center; line-height: 1.3; }
      .body.has-messages .welcome { display: none; }
      .messages { display: flex; flex-direction: column; gap: 8px; }
      .msg { max-width: 82%; padding: 8px 12px; border-radius: 14px; font-size: 14px;
             line-height: 1.4; white-space: pre-wrap; }
      .msg.user { align-self: flex-end; background: #ece5f4; color: #1a1a2e; border-bottom-right-radius: 4px; }
      .msg.bot { align-self: flex-start; background: #f2f2f5; color: #1a1a2e; border-bottom-left-radius: 4px; }
      .sources { font-size: 11px; color: #888; margin-top: 4px; }
      .input-area { flex-shrink: 0; padding: 8px 14px 14px; }
      .input-row { display: flex; align-items: center; gap: 6px; border: 1px solid #ddd;
                   border-radius: 24px; padding: 4px 4px 4px 16px; }
      input { flex: 1; border: none; outline: none; font-size: 14px; padding: 8px 4px;
              font-family: inherit; }
      .send { width: 32px; height: 32px; border-radius: 50%; border: none; background: #16161d;
              color: #fff; cursor: pointer; flex-shrink: 0; display: flex; align-items: center;
              justify-content: center; font-size: 14px; }
      .disclaimer { font-size: 11px; color: #8a8a8a; text-align: center; margin-top: 8px; line-height: 1.4; }
    </style>
    <div class="box">
      <div class="header">
        <span class="icon">💬</span>
        <span class="title">Kunnskapsboten</span>
        ${mockMode ? '<span class="badge">DEMO</span>' : ""}
        <span class="chevron">⌄</span>
      </div>
      <div class="body">
        <div class="welcome">Hva kan jeg hjelpe deg med i dag?</div>
        <div class="messages"></div>
      </div>
      <div class="input-area">
        <div class="input-row">
          <input type="text" placeholder="Skriv inn ditt spørsmål" />
          <button class="send" aria-label="Send">&#8593;</button>
        </div>
        <div class="disclaimer">Denne tjenesten er basert på kunstig intelligens, feil kan oppstå</div>
      </div>
    </div>
  `;

  const boxEl = root.querySelector(".box");
  const headerEl = root.querySelector(".header");
  const bodyEl = root.querySelector(".body");
  const messagesEl = root.querySelector(".messages");
  const inputEl = root.querySelector("input");
  const buttonEl = root.querySelector(".send");

  headerEl.addEventListener("click", () => boxEl.classList.toggle("collapsed"));

  function addMessage(text, sender) {
    bodyEl.classList.add("has-messages");
    const div = document.createElement("div");
    div.className = `msg ${sender}`;
    div.textContent = text;
    messagesEl.appendChild(div);
    bodyEl.scrollTop = bodyEl.scrollHeight;
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

  buttonEl.addEventListener("click", (e) => {
    e.stopPropagation();
    sendQuestion();
  });
  inputEl.addEventListener("click", (e) => e.stopPropagation());
  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") sendQuestion();
  });
})();
