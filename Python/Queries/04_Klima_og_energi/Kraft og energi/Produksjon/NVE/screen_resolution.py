from screeninfo import get_monitors
import ctypes

for m in get_monitors():
    print(f"Monitor: {m.name}, Resolution: {m.width}x{m.height}")

user32 = ctypes.windll.user32
gdi32 = ctypes.windll.gdi32

user32.SetProcessDPIAware()
hDC = user32.GetDC(0)
dpi = gdi32.GetDeviceCaps(hDC, 88)  # 88 = LOGPIXELSX

print(f"DPI: {dpi}")
print(f"Scaling: {dpi / 96 * 100:.0f}%")
