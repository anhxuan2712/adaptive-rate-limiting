# TÀI LIỆU CHUYÊN SÂU: GIẢI THÍCH CHI TIẾT CÁC THUẬT TOÁN, ĐẠI LƯỢNG, CÔNG THỨC VÀ VÍ DỤ TÍNH TOÁN

> **Tài liệu tham chiếu:** [Báo cáo nghiên cứu đề tài: Nghiên cứu và triển khai thuật toán Sliding Window kết hợp cơ chế Adaptive Threshold cho hệ thống Rate Limiting trên Kubernetes](file:///d:/%C4%90%E1%BB%81%20t%C3%A0i%20nghi%C3%AAn%20c%E1%BB%A9u/De_Tai_Nghien_Cuu_Rate_Limiting.md)

---

## MỤC LỤC TỔNG HỢP

1. [BẢNG TỔNG HỢP TOÀN BỘ CÁC ĐẠI LƯỢNG VÀ KÝ HIỆU TOÁN HỌC](#1-bảng-tổng-hợp-toàn-bộ-các-đại-lượng-và-ký-hiệu-toán-học)
2. [PHẦN I: CÁC THUẬT TOÁN RATE LIMITING TRUYỀN THỐNG](#phần-i-các-thuật-toán-rate-limiting-truyền-thống)
   - [1. Fixed Window Counter (Bộ đếm cửa sổ cố định)](#1-fixed-window-counter-bộ-đếm-cửa-sổ-cố-định)
   - [2. Leaky Bucket (Thùng rò rỉ)](#2-leaky-bucket-thùng-rò-rỉ)
   - [3. Token Bucket (Thùng thẻ bài)](#3-token-bucket-thùng-thẻ-bài)
   - [4. Sliding Window Log (Nhật ký cửa sổ trượt)](#4-sliding-window-log-nhật-ký-cửa-sổ-trượt)
3. [PHẦN II: THUẬT TOÁN DATA PLANE - SLIDING WINDOW COUNTER PHÂN TÁN](#phần-ii-thuật-toán-data-plane---sliding-window-counter-phân-tán)
   - [1. Nguyên lý nội suy tỷ trọng thời gian (Time-Window Interpolation)](#1-nguyên-lý-nội-suy-tỷ-trọng-thời-gian-time-window-interpolation)
   - [2. Công thức toán học tường minh](#2-công-thức-toán-học-tường-minh)
   - [3. Giải thích giải thuật Script Lua nguyên tử trên Redis](#3-giải-thích-giải-thuật-script-lua-nguyên-tử-trên-redis)
   - [4. Ví dụ tính toán số học từng bước (Step-by-step Execution)](#4-ví-dụ-tính-toán-số-học-từng-bước-step-by-step-execution)
4. [PHẦN III: THUẬT TOÁN CONTROL PLANE - ADAPTIVE THRESHOLD CONTROLLER (MAPE-K)](#phần-iii-thuật-toán-control-plane---adaptive-threshold-controller-mape-k)
   - [1. Trung bình trượt có trọng số mũ (EWMA)](#1-trung-bình-trượt-có-trọng-số-mũ-ewma)
   - [2. Độ lệch chuẩn trượt mẫu hiệu chỉnh (Rolling Sample StdDev)](#2-độ-lệch-chuẩn-trượt-mẫu-hiệu-chỉnh-rolling-sample-stddev)
   - [3. Công thức ngưỡng động & Bộ kẹp biên an toàn (Safety Clamping)](#3-công-thức-ngưỡng-động--bộ-kẹp-biên-an-toàn-safety-clamping)
   - [4. Phân tích ảnh hưởng của siêu tham số ($\alpha, k, W$)](#4-phân-tích-ảnh-hưởng-của-siêu-tham-số-alpha-k-w)
   - [5. Kịch bản mô phỏng số học hoàn chỉnh qua 10 chu kỳ điều khiển](#5-kịch-bản-mô-phỏng-số-học-hoàn-chỉnh-qua-10-chu-kỳ-điều-khiển)
5. [PHẦN IV: CÁC THUẬT TOÁN THÍCH ỨNG LIÊN QUAN (RELATED WORKS)](#phần-iv-các-thuật-toán-thích-ứng-liên-quan-related-works)
   - [1. Netflix Concurrency Limits (TCP Vegas Gradient Algorithm)](#1-netflix-concurrency-limits-tcp-vegas-gradient-algorithm)
   - [2. Google SRE Adaptive Throttling (Client-side Drop Probability)](#2-google-sre-adaptive-throttling-client-side-drop-probability)
6. [PHẦN V: CÁC ĐẠI LƯỢNG ĐO LƯỜNG VÀ ĐÁNH GIÁ THỰC NGHIỆM](#phần-v-các-đại-lượng-đo-lường-và-đánh-giá-thực-nghiệm)

---

# 1. BẢNG TỔNG HỢP TOÀN BỘ CÁC ĐẠI LƯỢNG VÀ KÝ HIỆU TOÁN HỌC

| Ký hiệu / Biến số | Đơn vị | Tên gọi chuyên môn | Ý nghĩa và vai trò trong hệ thống |
| :--- | :--- | :--- | :--- |
| $T$ | Giây (s) | Window Duration | Kích thước độ dài của một cửa sổ thời gian (ví dụ: $1\text{s}$ hoặc $60\text{s}$). |
| $t$ | Giây (s) / ms | Timestamp | Thời điểm hiện tại khi request gửi đến hoặc khi chu kỳ điều khiển kích hoạt. |
| $t_{\text{curr}}$ | Giây (s) | Current Window Start | Mốc thời gian bắt đầu của khối cửa sổ hiện tại ($t_{\text{curr}} = \lfloor \frac{t}{T} \rfloor \times T$). |
| $t_{\text{prev}}$ | Giây (s) | Previous Window Start | Mốc thời gian bắt đầu của khối cửa sổ liền trước ($t_{\text{prev}} = t_{\text{curr}} - T$). |
| $\text{Count}_{\text{curr}}$ | Số nguyên | Current Count | Số lượng request đã được chấp thuận trong khối hiện tại. |
| $\text{Count}_{\text{prev}}$ | Số nguyên | Previous Count | Tổng số lượng request đã được chấp thuận trong toàn bộ khối trước. |
| $\text{weight}$ | Số thực $[0, 1]$ | Time Decay Weight | Tỷ trọng phần chồng lấn thời gian của cửa sổ trước trong cửa sổ trượt hiện tại. |
| $\text{Count}_{\text{sliding}}$ | Số thực | Estimated Sliding Count | Ước lượng số request nằm trọn vẹn trong khoảng $[t - T, t]$. |
| $X(t)$ | RPS | Observed Traffic | Tốc độ request trung bình trên giây đo được tại chu kỳ điều khiển thứ $t$. |
| $\text{EWMA}(t)$ | RPS | Exponential Moving Average | Giá trị trung bình trượt làm mịn phản ánh xu hướng dài hạn của lưu lượng. |
| $\alpha$ | Số thực $(0, 1]$ | Smoothing Factor | Hệ số suy giảm trọng số mũ (quyết định độ nhạy của EWMA). |
| $W$ | Số nguyên | Rolling Window Size | Số lượng mẫu lịch sử dùng để tính độ lệch chuẩn (ví dụ: $W = 10$). |
| $\overline{X}_W(t)$ | RPS | Sample Mean | Giá trị trung bình số học của $W$ mẫu lưu lượng gần nhất. |
| $\text{StdDev}_W(t)$ / $s$ | RPS | Sample Standard Deviation | Độ lệch chuẩn mẫu hiệu chỉnh phản ánh mức độ biến động ngẫu nhiên của tải. |
| $k$ | Số thực $> 0$ | Tolerance Multiplier | Hệ số nhân dung sai độ lệch chuẩn (xác lập dải đệm tin cậy). |
| $\text{Threshold}_{\text{raw}}(t)$ | RPS | Raw Calculated Limit | Ngưỡng tính toán lý thuyết trước khi kẹp biên bảo vệ. |
| $\text{Threshold}_{\min}$ | RPS | Minimum Floor Limit | Ngưỡng sàn bảo vệ tối thiểu, ngăn chặn hạ ngưỡng quá thấp làm nghẽn dịch vụ. |
| $\text{Threshold}_{\max}$ | RPS | Maximum Ceiling Limit | Ngưỡng trần bảo vệ tối đa, tương ứng giới hạn chịu tải tới hạn của Backend. |
| $\text{Threshold}(t)$ | RPS | Final Applied Limit | Ngưỡng Rate Limit cuối cùng được nạp vào Kong Gateway qua Admin API. |
| $RTT_{\text{actual}}$ | ms | Actual Round-Trip Time | Thời gian đáp ứng thực tế đo được của dịch vụ downstream. |
| $RTT_{\text{no\_load}}$ | ms | Baseline / Min RTT | Thời gian phản hồi cơ sở lý tưởng khi dịch vụ không bị nghẽn tải. |
| $P_{\text{drop}}$ | Xác suất $[0, 1]$ | Drop Probability | Xác suất loại bỏ request tại phía Client trong thuật toán Google SRE. |
| $P_{\text{FP}}$ | Tỷ lệ $\%$ | False Positive Rate | Tỷ lệ các request hợp lệ bị Gateway từ chối nhầm ($HTTP 429$). |
| $p_{50}, p_{95}, p_{99}$ | ms | Percentile Latencies | Phân vị thời gian phản hồi của $50\%, 95\%, 99\%$ request. |

---

# PHẦN I: CÁC THUẬT TOÁN RATE LIMITING TRUYỀN THỐNG

```
+----------------------------------------------------------------------------------------------------+
|                                    BẢN ĐỒ SO SÁNH NGUYÊN LÝ THUẬT TOÁN                             |
+----------------------------------------------------------------------------------------------------+
| 1. Fixed Window:       [---- Window 1: Counter <= L ----] [---- Window 2: Counter <= L ----]       |
|                        --> Lỗ hổng: Giao điểm 2 cửa sổ có thể dồn 2L requests trong delta t.       |
|                                                                                                    |
| 2. Leaky Bucket:       Request vào (Bất định) ---> [ Hàng đợi FIFO (Dung tích C) ]                 |
|                                                                    | Rò rỉ cố định: r req/s        |
|                                                                    v                               |
|                                                               Backend xử lý (Không có Burst)       |
|                                                                                                    |
| 3. Token Bucket:       Bơm Token: R token/s ---> [ Thùng Token (Dung tích B) ]                     |
|                                                             |                                      |
|                        Request đến + Lấy 1 Token ---------> Chấp thuận (Cho phép Burst = B)        |
|                        Nếu thùng rỗng (0 token) ----------> Từ chối (HTTP 429)                     |
|                                                                                                    |
| 4. Sliding Window Log: [  x  x  xx   x    x   xx  x   ]  Lưu Timestamp mọi request vào RAM         |
|                        |<-------- Cửa sổ T --------->|   Đếm số phần tử trong khoảng [t-T, t]      |
|                                                                                                    |
| 5. Sliding Window Ctr: [ Block Cũ: Count_prev ] | [ Block Hiện Tại: Count_curr (t) ]               |
|                                  \                 /                                               |
|                                   \               /                                                |
|                                    Nội suy tỷ trọng: Count = Count_curr + Count_prev * (1 - dt/T)  |
+----------------------------------------------------------------------------------------------------+
```

---

## 1. Fixed Window Counter (Bộ đếm cửa sổ cố định)

### 1.1. Nguyên lý hoạt động
- Dòng thời gian được chia thành các khoảng cố định kích thước $T$ (ví dụ: $[0, 60\text{s}), [60\text{s}, 120\text{s}), \dots$).
- Mỗi cửa sổ có một bộ đếm $\text{Counter}$ riêng khởi tạo bằng 0.
- Khi một request đến tại thời điểm $t$, hệ thống xác định ID cửa sổ: $\text{Window ID} = \lfloor \frac{t}{T} \rfloor$.
- Nếu $\text{Counter} < L$ (với $L$ là giới hạn cấu hình), tăng $\text{Counter} \leftarrow \text{Counter} + 1$ và chấp nhận request.
- Nếu $\text{Counter} \ge L$, từ chối request ($HTTP 429$). Khi thời gian bước sang cửa sổ mới, $\text{Counter}$ được reset về 0.

### 1.2. Công thức toán học
$$\text{Window ID}(t) = \lfloor \frac{t}{T} \rfloor$$
$$\text{Decision}(t) = \begin{cases} \text{ALLOW}, & \text{nếu } \text{Counter}_{\text{Window ID}(t)} < L \\ \text{DENY (429)}, & \text{nếu } \text{Counter}_{\text{Window ID}(t)} \ge L \end{cases}$$

### 1.3. Phân tích nhược điểm chí mạng: Lỗi biên (Boundary Burst Problem)
Giả sử $T = 60\text{s}$ và $L = 100 \text{ req/phút}$.
- Trong Cửa sổ 1 ($[0\text{s}, 60\text{s}]$): Từ giây $0$ đến giây $58$, không có request nào. Đến giây $59$, client gửi **100 requests** $\rightarrow$ Hệ thống cho phép 100 requests ($\text{Counter}_1 = 100$).
- Trong Cửa sổ 2 ($[60\text{s}, 120\text{s}]$): Lúc $60.01\text{s}$, bộ đếm reset về 0. Client tiếp tục gửi ngay **100 requests** $\rightarrow$ Hệ thống cho phép tiếp 100 requests ($\text{Counter}_2 = 100$).
- **Hậu quả:** Trong khoảng thời gian cực ngắn chỉ $2\text{s}$ (từ giây $59$ đến giây $61$), Backend đã phải hứng chịu **200 requests**, gấp **200% năng lực chịu tải tối đa**, có thể làm sập Thread Pool của Backend Microservice ngay lập tức.

---

## 2. Leaky Bucket (Thùng rò rỉ)

### 2.1. Nguyên lý hoạt động
- Hoạt động tương tự như một chiếc thùng có lỗ thủng ở đáy:
  - Nước đổ vào thùng với lưu lượng bất kỳ đại diện cho các HTTP Requests đến Gateway.
  - Thùng có dung tích tối đa $C$ (sức chứa hàng đợi Queue Capacity).
  - Nước rò rỉ ra ngoài đáy thùng với tốc độ đều đặn không đổi $r$ (Leak Rate - số request xử lý trên giây).
- Nếu request đến khi hàng đợi chưa đầy ($q < C$), request được xếp vào hàng đợi chờ xử lý.
- Nếu request đến khi hàng đợi đã đầy ($q = C$), request bị tràn (Overflow) và bị từ chối ngay lập tức ($HTTP 429$).

### 2.2. Công thức toán học
Gọi $q(t)$ là số lượng request đang nằm trong hàng đợi tại thời điểm $t$. Khi có một request mới đến tại thời điểm $t$, sau khoảng thời gian $\Delta t = t - t_{\text{last}}$ kể từ request trước:
$$q(t) = \max\Big(0, \; q(t_{\text{last}}) - r \times \Delta t\Big)$$
$$\text{Decision}(t) = \begin{cases} \text{ALLOW (Enqueued)}, & \text{nếu } q(t) + 1 \le C \quad (\text{khi đó } q(t) \leftarrow q(t) + 1) \\ \text{DENY (Dropped 429)}, & \text{nếu } q(t) + 1 > C \end{cases}$$

### 2.3. Ví dụ tính toán số học
Giả sử cấu hình: Dung tích hàng đợi $C = 5$ requests, tốc độ rò rỉ $r = 2 \text{ req/s}$ ($0.5\text{s}$ xử lý xong 1 request).
- **$t = 0.0\text{s}$:** Có 5 requests gửi đến cùng lúc.
  - $q(0) = 5$ (đầy thùng). Cả 5 request được nhận vào hàng đợi.
- **$t = 0.2\text{s}$:** Thêm 1 request thứ 6 đến.
  - Lượng rò rỉ trong $0.2\text{s}$: $r \times \Delta t = 2 \times 0.2 = 0.4 \text{ req}$ (chưa đủ xử lý xong 1 request).
  - $q(0.2) = 5 - 0.4 = 4.6 \rightarrow$ Làm tròn xuống số nguyên đang giữ là 5.
  - Request thứ 6 bị **DROP (429)** vì thùng vẫn đầy.
- **$t = 1.0\text{s}$:** Thời gian trôi qua $1\text{s}$, đã có $2 \times 1.0 = 2$ request được xử lý xong $\rightarrow q(1.0) = 5 - 2 = 3$. Thùng còn trống 2 chỗ.
- **Hạn chế:** Thuật toán triệt tiêu hoàn toàn khả năng xử lý bùng nổ lưu lượng hợp lệ (Burst Traffic). Dù Backend có rảnh rỗi đến đâu, tốc độ phục vụ tối đa vẫn bị gông cứng tại mức $r$.

---

## 3. Token Bucket (Thùng thẻ bài)

### 3.1. Nguyên lý hoạt động
- Một thùng chứa token có dung tích tối đa là $B$ (Burst Capacity).
- Token được bổ sung liên tục vào thùng với tốc độ cố định $R$ token/giây (Refill Rate). Nếu thùng đầy $B$, token sinh ra thêm sẽ bị tràn và bỏ qua.
- Mỗi khi có 1 request đến:
  - Nếu thùng có $\ge 1$ token: Lấy 1 token ra khỏi thùng và cho phép request đi tiếp tới Backend.
  - Nếu thùng hết token ($0$ token): Request bị chặn ($HTTP 429$).
- **Điểm ưu việt:** Cho phép hệ thống xử lý một đợt bùng nổ tải tức thời lên tới $B$ requests trong thời gian $0\text{s}$, sau đó tốc độ sẽ tự động hạ về ổn định ở mức $R$ req/s.

### 3.2. Công thức toán học
Gọi $b(t)$ là số lượng token hiện có trong thùng tại thời điểm $t$, $t_{\text{last}}$ là thời điểm cập nhật token gần nhất:
$$b_{\text{refill}}(t) = \min\Big(B, \; b(t_{\text{last}}) + R \times (t - t_{\text{last}})\Big)$$
$$\text{Decision}(t) = \begin{cases} \text{ALLOW}, & \text{nếu } b_{\text{refill}}(t) \ge 1 \quad (\text{khi đó } b(t) \leftarrow b_{\text{refill}}(t) - 1, \; t_{\text{last}} \leftarrow t) \\ \text{DENY (429)}, & \text{nếu } b_{\text{refill}}(t) < 1 \quad (\text{khi đó } b(t) \leftarrow b_{\text{refill}}(t), \; t_{\text{last}} \leftarrow t) \end{cases}$$

### 3.3. Ví dụ tính toán số học
Cấu hình: Dung tích thùng $B = 10$ tokens, Tốc độ nạp $R = 5$ tokens/giây. Ban đầu tại $t = 0\text{s}$, thùng đầy $b(0) = 10$.
1. **Tại $t = 0.0\text{s}$:** 10 requests ập đến đồng thời.
   - Thùng cấp 10 token cho 10 requests. Cả 10 requests đều được **ALLOW**.
   - Số token còn lại: $b = 0$.
2. **Tại $t = 0.1\text{s}$:** Có 1 request đến.
   - Lượng token nạp thêm: $R \times \Delta t = 5 \times 0.1 = 0.5$ token.
   - Tổng token hiện có: $0 + 0.5 = 0.5 < 1$ token $\rightarrow$ **DENY (HTTP 429)**.
3. **Tại $t = 0.6\text{s}$:** Có 2 requests đến.
   - Khoảng thời gian từ $t=0.0\text{s}$ là $\Delta t = 0.6\text{s}$.
   - Lượng token nạp: $5 \times 0.6 = 3.0$ tokens.
   - Request 1 lấy 1 token $\rightarrow$ ALLOW (còn 2.0).
   - Request 2 lấy 1 token $\rightarrow$ ALLOW (còn 1.0).
   - Cả 2 request được **ALLOW**, thùng còn $1.0$ token.

---

## 4. Sliding Window Log (Nhật ký cửa sổ trượt)

### 4.1. Nguyên lý hoạt động
- Ghi nhận và lưu trữ chính xác Timestamp $t_i$ của **mọi request** đã đến vào một cấu trúc dữ liệu sắp xếp theo thời gian (như Sorted Set trong Redis).
- Mỗi khi có request mới đến tại thời điểm $t$:
  1. Thêm timestamp $t$ vào danh sách.
  2. Quét và loại bỏ tất cả các log timestamp cũ hơn khoảng thời gian xét: $t_i < (t - T)$.
  3. Đếm tổng số log còn lại trong tập hợp.
  4. Nếu $\text{Total} \le L$, cho phép request. Nếu $\text{Total} > L$, từ chối request.

### 4.2. Phân tích công thức và độ phức tạp
- **Tập hợp log tại thời điểm $t$:**
  $$S(t) = \{ t_i \in \text{Logs} \mid t - T < t_i \le t \}$$
- **Quyết định:** $\text{Decision} = \text{ALLOW}$ nếu $|S(t)| \le L$, ngược lại $\text{DENY}$.
- **Độ phức tạp tính toán:**
  - Thời gian: $O(\log N + M)$ với $M$ là số phần tử bị xóa.
  - **Không gian (Bộ nhớ): $O(N)$** với $N$ là tổng số request gửi đến trong cửa sổ $T$.
- **Tại sao không khả thi trên quy mô lớn?**
  - Nếu hệ thống chịu tải $10,000\text{ RPS}$ với cửa sổ $T = 60\text{s}$, Redis phải lưu trữ $10,000 \times 60 = 600,000$ mục timestamp cho mỗi API route. Với hàng ngàn API route và hàng chục ngàn người dùng, RAM của Redis sẽ bị cạn kiệt nhanh chóng, dẫn đến nghẽn CPU do thao tác xóa mảng lớn liên tục.

---

# PHẦN II: THUẬT TOÁN DATA PLANE - SLIDING WINDOW COUNTER PHÂN TÁN

Đây là thuật toán cốt lõi được lựa chọn và hiện thực hóa tại tầng Ingress Gateway trong đề tài nghiên cứu, kết hợp hoàn hảo giữa độ chính xác cao của Sliding Window Log và chi phí bộ nhớ tối thiểu $O(1)$ của Fixed Window Counter.

```
Dòng thời gian:
|<----------------- Cửa sổ trước (T) ----------------->|<---------------- Cửa sổ hiện tại (T) --------------->|
+------------------------------------------------------+------------------------------------------------------+
| Key: rl:prev:backend:1000                            | Key: rl:curr:backend:1060                            |
| Tổng số request đã đếm: Count_prev = 80             | Đang đếm: Count_curr = 30                            |
+------------------------------------------------------+------------------------------------------------------+
                                                       |               t = 1075s                              |
                                                       |               (Đã trôi qua 15s trong block mới)     |
                                                       |<--- 15s ----->|                                      |
                                                       |               |                                      |
       |<--------------------------------- CỬA SỔ TRƯỢT T = 60s ------------------------------>|               |
       |                              Khoảng thời gian: [1015s -> 1075s]                       |               |
       |<----------------- 45s còn lại --------------->|<------------- 15s đã qua ------------>|               |
       |  Tỷ trọng thời gian: weight = 45/60 = 0.75    |  Trọng số 100%: 30 requests           |               |
       |  Số request đóng góp: 80 * 0.75 = 60          |                                       |               |
       +-----------------------------------------------+---------------------------------------+               |
       ==> ƯỚC LƯỢNG CỬA SỔ TRƯỢT: Estimated_Count = 30 + (80 * 0.75) = 90 requests                             |
```

---

## 1. Nguyên lý nội suy tỷ trọng thời gian (Time-Window Interpolation)

Thay vì lưu từng log timestamp chi tiết, thuật toán chia thời gian thành các khối cố định độ dài $T$ và chỉ lưu **duy nhất 2 con số nguyên (Counter)** trên Redis:
1. `Count_prev`: Số lượng request của khối thời gian trước đó.
2. `Count_curr`: Số lượng request của khối thời gian hiện tại.

Khi một request đến tại thời điểm $t$ bất kỳ nằm trong khối hiện tại, một phần của cửa sổ trượt $60\text{s}$ sẽ nằm ở khối trước, và phần còn lại nằm ở khối hiện tại. Thuật toán giả định rằng **các request trong khối trước phân phối tương đối đồng đều theo thời gian**, từ đó tính số request của khối trước đóng góp vào cửa sổ trượt bằng phương pháp nội suy tuyến tính theo tỷ lệ thời gian chồng lấn.

---

## 2. Công thức toán học tường minh

1. **Xác định mốc thời gian bắt đầu của 2 khối:**
   $$t_{\text{curr}} = \lfloor \frac{t}{T} \rfloor \times T$$
   $$t_{\text{prev}} = t_{\text{curr}} - T$$

2. **Tính tỷ trọng thời gian chồng lấn của khối trước ($\text{weight}$):**
   Thời gian đã trôi qua trong khối hiện tại là $(t - t_{\text{curr}})$. Do đó, phần thời gian của khối trước còn nằm trong cửa sổ trượt là $T - (t - t_{\text{curr}})$.
   $$\text{weight} = \frac{T - (t - t_{\text{curr}})}{T} = 1 - \frac{t - t_{\text{curr}}}{T}$$
   *(Đặc điểm: $\text{weight} \in [0, 1]$; khi mới bước vào khối mới $t \approx t_{\text{curr}} \Rightarrow \text{weight} \approx 1$; khi sắp hết khối $t \to t_{\text{curr}} + T \Rightarrow \text{weight} \to 0$)*

3. **Công thức ước lượng số request trong cửa sổ trượt:**
   $$\text{Estimated Count}(t) = \text{Count}_{\text{curr}} + \Big( \text{Count}_{\text{prev}} \times \text{weight} \Big)$$

4. **Quyết định kiểm soát lưu lượng (Admission Control):**
   $$\text{Decision}(t) = \begin{cases} \text{ALLOW (HTTP 200)}, & \text{nếu } \text{Estimated Count}(t) < \text{Threshold}(t) \\ \text{DENY (HTTP 429)}, & \text{nếu } \text{Estimated Count}(t) \ge \text{Threshold}(t) \end{cases}$$

---

## 3. Giải thích giải thuật Script Lua nguyên tử trên Redis

Trong hệ thống phân tán Kubernetes với hàng chục Pod Kong Ingress chạy song song, việc thực hiện lần lượt các lệnh `GET prev`, `GET curr`, tính toán và `INCR curr` từ ứng dụng sẽ gây ra **Race Condition** (nhiều Pod cùng đọc một giá trị và cùng cho phép vượt ngưỡng) và tốn nhiều Round-Trip Time (RTT).

Giải pháp là đóng gói toàn bộ logic vào **1 Lua Script duy nhất** chạy trực tiếp trong lõi đơn luồng của Redis.

```lua
-- KEYS[1]: Key khối trước (Ví dụ: "rl:backend:1000")
-- KEYS[2]: Key khối hiện tại (Ví dụ: "rl:backend:1060")
-- ARGV[1]: Tỷ trọng weight (Ví dụ: "0.75")
-- ARGV[2]: Ngưỡng giới hạn Threshold(t) (Ví dụ: "100")
-- ARGV[3]: Thời gian sống TTL tính bằng giây (Ví dụ: "120" = 2 * T)

local prev_key = KEYS[1]
local curr_key = KEYS[2]
local weight = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- BƯỚC 1: Đọc giá trị 2 bộ đếm (nếu chưa có thì mặc định bằng 0)
local prev_count = tonumber(redis.call('GET', prev_key) or "0")
local curr_count = tonumber(redis.call('GET', curr_key) or "0")

-- BƯỚC 2: Tính toán nội suy số request trong cửa sổ trượt
local estimated_count = curr_count + (prev_count * weight)

-- BƯỚC 3: So sánh với ngưỡng cho phép
if estimated_count < limit then
    -- Thỏa mãn: Tăng bộ đếm của khối hiện tại thêm 1
    redis.call('INCR', curr_key)
    
    -- Nếu là request đầu tiên của khối mới, đặt TTL để Redis tự động giải phóng RAM sau 2 chu kỳ
    if curr_count == 0 then
        redis.call('EXPIRE', curr_key, ttl)
    end
    return 1 -- Trả về 1: Cho phép đi tiếp (ALLOW - HTTP 200)
else
    return 0 -- Trả về 0: Từ chối ngay tại Ingress (DENY - HTTP 429)
end
```

### Tại sao giải thuật này đạt hiệu năng vượt trội?
1. **Tính nguyên tử tuyệt đối (Atomicity):** Redis thực thi script Lua tuần tự trong single-thread. Không có bất kỳ request nào có thể xen ngang giữa bước đọc `GET` và tăng `INCR`, loại bỏ hoàn toàn 100% hiện tượng Race Condition.
2. **Tối ưu Network I/O (1 RTT duy nhất):** Kong Gateway chỉ gửi 1 lệnh `EVALSHA` duy nhất tới Redis và nhận về kết quả nhị phân (`1` hoặc `0`), độ trễ chỉ mất **$0.4 - 1.2 \text{ ms}$**.
3. **Bộ nhớ tối ưu tuyệt đối $O(1)$:** Mỗi route chỉ lưu đúng 2 keys dạng Integer trên Redis (tốn chưa đầy $100 \text{ bytes}$ RAM). Cơ chế `EXPIRE` tự động dọn dẹp các key quá hạn.

---

## 4. Ví dụ tính toán số học từng bước (Step-by-step Execution)

**Thiết lập tham số:**
- Cửa sổ thời gian: $T = 60\text{s}$.
- Ngưỡng giới hạn cho phép: $\text{Threshold} = 100 \text{ requests/phút}$.
- Khối trước ($[0\text{s}, 60\text{s}]$): Đã tiếp nhận và ghi nhận $\text{Count}_{\text{prev}} = 80 \text{ requests}$.
- Khối hiện tại ($[60\text{s}, 120\text{s}]$): Bắt đầu tại $t_{\text{curr}} = 60\text{s}$.

Hãy theo dõi diễn biến xử lý tại các mốc thời gian cụ thể:

### Mốc 1: Tại thời điểm $t = 75\text{s}$ (Đã qua 15s của khối mới, $\text{Count}_{\text{curr}} = 30$)
1. Tính tỷ trọng $\text{weight}$:
   $$\text{weight} = 1 - \frac{75 - 60}{60} = 1 - \frac{15}{60} = 1 - 0.25 = 0.75$$
2. Tính số lượng ước lượng trong cửa sổ trượt $[15\text{s}, 75\text{s}]$:
   $$\text{Estimated Count} = 30 + (80 \times 0.75) = 30 + 60 = 90 \text{ requests}$$
3. Kiểm tra điều kiện:
   $$\text{Estimated Count} = 90 < 100 \implies \mathbf{ALLOW} \quad (\text{Sau đó } \text{Count}_{\text{curr}} \text{ tăng lên } 31)$$

---

### Mốc 2: Tại thời điểm $t = 90\text{s}$ (Đã qua 30s của khối mới, $\text{Count}_{\text{curr}} = 65$)
1. Tính tỷ trọng $\text{weight}$:
   $$\text{weight} = 1 - \frac{90 - 60}{60} = 1 - \frac{30}{60} = 1 - 0.50 = 0.50$$
2. Tính số lượng ước lượng trong cửa sổ trượt $[30\text{s}, 90\text{s}]$:
   $$\text{Estimated Count} = 65 + (80 \times 0.50) = 65 + 40 = 105 \text{ requests}$$
3. Kiểm tra điều kiện:
   $$\text{Estimated Count} = 105 \ge 100 \implies \mathbf{DENY \; (HTTP \; 429)}$$
   *(Request bị từ chối ngay lập tức, $\text{Count}_{\text{curr}}$ giữ nguyên là 65, không tăng).*

---

### Mốc 3: Tại thời điểm $t = 114\text{s}$ (Đã qua 54s của khối mới, $\text{Count}_{\text{curr}} = 85$)
1. Tính tỷ trọng $\text{weight}$:
   $$\text{weight} = 1 - \frac{114 - 60}{60} = 1 - \frac{54}{60} = 1 - 0.90 = 0.10$$
2. Tính số lượng ước lượng trong cửa sổ trượt $[54\text{s}, 114\text{s}]$:
   $$\text{Estimated Count} = 85 + (80 \times 0.10) = 85 + 8 = 93 \text{ requests}$$
3. Kiểm tra điều kiện:
   $$\text{Estimated Count} = 93 < 100 \implies \mathbf{ALLOW} \quad (\text{Sau đó } \text{Count}_{\text{curr}} \text{ tăng lên } 86)$$

> **Nhận xét quan trọng:** Thuật toán làm mịn dòng lưu lượng một cách hoàn hảo. Càng về cuối khối hiện tại, ảnh hưởng của khối cũ càng giảm dần về 0, loại bỏ hoàn toàn hiện tượng bùng nổ lưu lượng gấp đôi tại biên như Fixed Window Counter.

---

# PHẦN III: THUẬT TOÁN CONTROL PLANE - ADAPTIVE THRESHOLD CONTROLLER (MAPE-K)

Control Plane hoạt động ngoài băng (*Out-of-band*), định kỳ thức dậy mỗi $\Delta t_{\text{interval}} = 15\text{s}$ để thu thập chỉ số từ Prometheus, thực hiện tính toán thống kê và cập nhật ngưỡng Rate Limit mới cho Kong API Gateway.

```
       +-----------------------------------------------------------------------------------+
       |                    CHU TRÌNH ĐIỀU KHIỂN THÍCH ỨNG MAPE-K                          |
       +-----------------------------------------------------------------------------------+
       |                                                                                   |
       |   [1. MONITOR]                                                                    |
       |   Prometheus Scrape Metrics từ Pods / Kong                                        |
       |   Query PromQL: sum(rate(kong_http_requests_total[1m])) ---> Lấy RPS mẫu: X(t)    |
       |                                       |                                           |
       |                                       v                                           |
       |   [2. ANALYZE]                                                                    |
       |   1. Cập nhật EWMA:     EWMA(t) = α * X(t) + (1 - α) * EWMA(t-1)                  |
       |   2. Cập nhật Window:   History = [X(t-9), ..., X(t)] (W = 10)                    |
       |   3. Tính StdDev:       StdDev_W(t) = sqrt( Variance(History) )                   |
       |                                       |                                           |
       |                                       v                                           |
       |   [3. PLAN]                                                                       |
       |   1. Tính ngưỡng thô:   Threshold_raw = EWMA(t) + k * StdDev_W(t)                 |
       |   2. Kẹp biên an toàn:  Threshold(t) = Clamp(Threshold_raw, Min_Limit, Max_Limit) |
       |                                       |                                           |
       |                                       v                                           |
       |   [4. EXECUTE]                                                                    |
       |   So sánh với ngưỡng cũ: Nếu Threshold(t) != Last_Applied:                        |
       |   Gọi Kong Admin API:   PATCH /plugins/{id} {"config.second": Threshold(t)}       |
       +-----------------------------------------------------------------------------------+
```

---

## 1. Trung bình trượt có trọng số mũ (EWMA)

### 1.1. Bản chất và ý nghĩa
EWMA (*Exponentially Weighted Moving Average*) là một kỹ thuật lọc số liệu chuỗi thời gian đệ quy, gán trọng số giảm dần theo hàm mũ cho các quan sát trong quá khứ.
- Giúp hệ thống nắm bắt được **xu hướng tải chủ đạo (Trend)** của hệ thống.
- Lọc bỏ các xung nhiễu ngẫu nhiên ngắn hạn trong tích tắc của mạng máy tính.

### 1.2. Công thức toán học
$$\text{EWMA}(t) = \alpha \cdot X(t) + (1 - \alpha) \cdot \text{EWMA}(t-1)$$

*Khởi tạo ban đầu:* $\text{EWMA}(0) = X(0)$.

### 1.3. Khai triển chuỗi toán học
Nếu khai triển đệ quy ngược về quá khứ $n$ bước:
$$\text{EWMA}(t) = \alpha X(t) + \alpha(1-\alpha)X(t-1) + \alpha(1-\alpha)^2 X(t-2) + \dots + (1-\alpha)^n \text{EWMA}(t-n)$$
Tổng các hệ số trọng số luôn bằng 1:
$$\alpha \sum_{i=0}^{\infty} (1-\alpha)^i = \alpha \cdot \frac{1}{1 - (1 - \alpha)} = 1$$

---

## 2. Độ lệch chuẩn trượt mẫu hiệu chỉnh (Rolling Sample StdDev)

### 2.1. Bản chất và ý nghĩa
Trong khi $\text{EWMA}$ đo lường mức độ trung tâm (kỳ vọng tải), $\text{StdDev}$ đo lường **độ phân tán và biên độ dao động tự nhiên** của lưu lượng truy cập.
- Khi tải ổn định đều đặn: $\text{StdDev} \approx 0$.
- Khi tải bắt đầu có biến động, lượng người dùng ra vào thất thường: $\text{StdDev}$ tăng cao.

### 2.2. Công thức toán học
Xét cửa sổ trượt gồm $W$ mẫu dữ liệu RPS gần nhất: $\mathcal{H}_W = \{X(t-W+1), X(t-W+2), \dots, X(t)\}$.

1. **Trung bình số học trong cửa sổ $W$ ($\overline{X}_W(t)$):**
   $$\overline{X}_W(t) = \frac{1}{W} \sum_{i=0}^{W-1} X(t-i)$$

2. **Phương sai mẫu hiệu chỉnh ($s^2$ - Áp dụng hiệu chỉnh Bessel chia cho $W-1$ để ước lượng không chệch):**
   $$s_W^2(t) = \frac{1}{W - 1} \sum_{i=0}^{W-1} \Big( X(t-i) - \overline{X}_W(t) \Big)^2$$

3. **Độ lệch chuẩn mẫu hiệu chỉnh ($\text{StdDev}_W(t)$):**
   $$\text{StdDev}_W(t) = \sqrt{s_W^2(t)} = \sqrt{\frac{1}{W - 1} \sum_{i=0}^{W-1} \Big( X(t-i) - \overline{X}_W(t) \Big)^2}$$

---

## 3. Công thức ngưỡng động & Bộ kẹp biên an toàn (Safety Clamping)

### 3.1. Công thức ngưỡng thô (Raw Dynamic Threshold)
$$\text{Threshold}_{\text{raw}}(t) = \text{EWMA}(t) + k \cdot \text{StdDev}_W(t)$$

*Ý nghĩa của thành phần $k \cdot \text{StdDev}_W(t)$:*
- Đóng vai trò là một **dải đệm an toàn động (Dynamic Buffer)**.
- Khi hệ thống có độ biến động tải cao, dải đệm tự động nới rộng ra để chứa đựng các biến động này mà không chặn nhầm người dùng.
- Theo bất đẳng thức Chebyshev và phân phối chuẩn Gaussian:
  - Với $k = 1.0$: Bao phủ $\approx 68.27\%$ các dao động thông thường.
  - Với $k = 2.0$: Bao phủ $\approx 95.45\%$ các dao động thông thường (Khoảng tin cậy $95\%$).
  - Với $k = 3.0$: Bao phủ $\approx 99.73\%$ các dao động thông thường.

### 3.2. Bộ kẹp biên an toàn hai đầu (Dual-boundary Clamping)
Để loại trừ các rủi ro hệ thống rơi vào trạng thái cực đoan, giá trị ngưỡng bắt buộc phải đi qua hàm kẹp biên:
$$\text{Threshold}(t) = \text{clamp}\Big( \text{Threshold}_{\text{raw}}(t), \; \text{Threshold}_{\min}, \; \text{Threshold}_{\max} \Big)$$
Tương đương với:
$$\text{Threshold}(t) = \min\Big(\text{Threshold}_{\max}, \; \max\big(\text{Threshold}_{\min}, \; \text{Threshold}_{\text{raw}}(t)\big)\Big)$$

```
               Trục giá trị Threshold (RPS)
               ^
               |   [VÙNG NGUY HIỂM: Quá tải Backend - Gây sập sụp đổ dây chuyền]
 Threshold_max +================================================================ (Ví dụ: 300 RPS)
               |   
               |      /~~~~~~\      /\        <--- VÙNG HOẠT ĐỘNG THÍCH ỨNG DỘNG
               |     /        \    /  \            Threshold(t) biến thiên tự do
               |    /          \__/    \
 Threshold_min +================================================================ (Ví dụ: 50 RPS)
               |   [VÙNG RỦI RO: Khóa chặt dịch vụ - Không cho khách hàng truy cập]
               +----------------------------------------------------------------> Thời gian (t)
```

1. **Ý nghĩa của $\text{Threshold}_{\min}$ (Ngưỡng sàn):**
   - Giả sử vào ban đêm lúc 3 giờ sáng, lưu lượng giảm về gần $0 \text{ RPS}$. Nếu không có $\text{Threshold}_{\min}$, ngưỡng sẽ bị hạ xuống $0$ hoặc $1 \text{ RPS}$. Sáng hôm sau, chỉ cần vài người dùng đầu tiên truy cập sẽ bị Gateway chặn đứng ($HTTP 429$). Ngưỡng sàn đảm bảo luôn có một dung lượng tối thiểu để đón nhận người dùng.
2. **Ý nghĩa của $\text{Threshold}_{\max}$ (Ngưỡng trần):**
   - Tương ứng với năng lực tính toán tới hạn của Backend (ví dụ Backend Spring Boot tối đa chỉ chịu được $300\text{ RPS}$).
   - Trong các cuộc tấn công DDoS với lưu lượng $1200\text{ RPS}$, dù $\text{EWMA}$ và $\text{StdDev}$ có tăng cao đến đâu, bộ kẹp biên sẽ chốt cứng ngưỡng tại $300\text{ RPS}$, ép Gateway chặn đứng toàn bộ lưu lượng dư thừa ($900\text{ RPS}$) tại cửa ngõ.

---

## 4. Phân tích ảnh hưởng của siêu tham số ($\alpha, k, W$)

```
+---------------+------------------------+--------------------------------------------------------------------+
| Tham số       | Giá trị thực nghiệm    | Ảnh hưởng chi tiết tới hành vi hệ thống                            |
+---------------+------------------------+--------------------------------------------------------------------+
|               | α = 0.1 (Quá nhỏ)      | Ngưỡng phản ứng rất trễ, mất 45-60s mới nhận diện được Spike tải.  |
| Hệ số làm mịn | α = 0.8 (Quá lớn)      | Ngưỡng rung lắc mạnh theo từng dao động nhỏ của mạng (Over-react). |
|     (α)       | α = 0.3 (KHUYẾN NGHỊ)  | Cân bằng tối ưu giữa việc lọc nhiễu mạng và tốc độ bắt kịp tải mới.|
+---------------+------------------------+--------------------------------------------------------------------+
|               | k = 1.0 (Quá hẹp)      | Dải đệm quá sát trung bình, gây chặn nhầm ~15% request khi tải rung|
| Hệ số dung sai| k = 3.5 (Quá rộng)     | Dải đệm quá lỏng lẻo, phản ứng chậm khi bị tấn công quá tải.       |
|     (k)       | k = 2.0 (KHUYẾN NGHỊ)  | Khoảng tin cậy chuẩn 95%, triệt tiêu chặn nhầm, an toàn tuyệt đối. |
+---------------+------------------------+--------------------------------------------------------------------+
| Kích thước cửa| W = 3 (Quá ngắn)       | Mẫu quá ít, độ lệch chuẩn bị nhiễu cục bộ và mất tính đại diện.    |
|   sổ mẫu (W)  | W = 30 (Quá dài)       | Giữ lại dữ liệu quá cũ (450s), làm chậm khả năng hạ ngưỡng về sau. |
|               | W = 10 (KHUYẾN NGHỊ)   | Quan sát 150s lịch sử gần nhất, phản ánh trung thực hiện trạng tải.|
+---------------+------------------------+--------------------------------------------------------------------+
```

---

## 5. Kịch bản mô phỏng số học hoàn chỉnh qua 10 chu kỳ điều khiển

**Cấu hình hệ thống:**
- Chu kỳ điều khiển: $\Delta t = 15\text{s}$.
- Kích thước cửa sổ: $W = 5$ mẫu.
- Siêu tham số: $\alpha = 0.3$, $k = 2.0$.
- Biên an toàn: $\text{Threshold}_{\min} = 50 \text{ RPS}$, $\text{Threshold}_{\max} = 300 \text{ RPS}$.

Dưới đây là bảng tính toán số học chi tiết từng bước mô phỏng thực tế qua 3 giai đoạn: **Tải ổn định $\rightarrow$ Đột biến Spike $\rightarrow$ Tấn công DDoS**:

| Chu kỳ $t$ | RPS đo được $X(t)$ | $\text{EWMA}(t)$ (Công thức $\alpha=0.3$) | Lịch sử $W=5$ mẫu gần nhất | Mean $\overline{X}_5$ | $\text{StdDev}_5$ ($s$) | $\text{Threshold}_{\text{raw}}$ ($\text{EWMA} + 2s$) | Ngưỡng áp dụng $\text{Threshold}(t)$ | Trạng thái hệ thống |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **0** | $60.0$ | $60.00$ | $[60.0]$ | $60.00$ | $0.00$ | $60.00$ | **$60$ RPS** | Khởi tạo ban đầu |
| **1** | $62.0$ | $0.3(62) + 0.7(60.0) = \mathbf{60.60}$ | $[60.0, 62.0]$ | $61.00$ | $1.41$ | $60.60 + 2(1.41) = 63.42$ | **$63$ RPS** | Tải bình thường ổn định |
| **2** | $58.0$ | $0.3(58) + 0.7(60.6) = \mathbf{59.82}$ | $[60, 62, 58]$ | $60.00$ | $2.00$ | $59.82 + 2(2.00) = 63.82$ | **$64$ RPS** | Tải bình thường ổn định |
| **3** | $61.0$ | $0.3(61) + 0.7(59.82) = \mathbf{60.17}$ | $[60, 62, 58, 61]$ | $60.25$ | $1.71$ | $60.17 + 2(1.71) = 63.59$ | **$64$ RPS** | Tải bình thường ổn định |
| **4** | $59.0$ | $0.3(59) + 0.7(60.17) = \mathbf{59.82}$ | $[60, 62, 58, 61, 59]$ | $60.00$ | $1.58$ | $59.82 + 2(1.58) = 62.98$ | **$63$ RPS** | Đủ 5 mẫu lịch sử |
| **5** | **$180.0$** | $0.3(180) + 0.7(59.82) = \mathbf{95.87}$ | $[62, 58, 61, 59, 180]$ | $84.00$ | $53.74$ | $95.87 + 2(53.74) = \mathbf{203.35}$ | **$203$ RPS** | ⚡ **SPIKE BẮT ĐẦU: Ngưỡng tự nới lỏng** |
| **6** | **$240.0$** | $0.3(240) + 0.7(95.87) = \mathbf{139.11}$| $[58, 61, 59, 180, 240]$| $119.60$| $84.09$ | $139.11 + 2(84.09) = \mathbf{307.29}$| **$300$ RPS** | ⚡ **Đỉnh Spike: Chạm trần $\text{Threshold}_{\max}$** |
| **7** | $250.0$ | $0.3(250) + 0.7(139.11) = \mathbf{172.38}$| $[61, 59, 180, 240, 250]$| $158.00$| $94.68$ | $172.38 + 2(94.68) = \mathbf{361.74}$| **$300$ RPS** | Kẹp trần an toàn $300\text{ RPS}$ |
| **8** | **$1200.0$**| $0.3(1200) + 0.7(172.38) = \mathbf{480.67}$| $[59, 180, 240, 250, 1200]$| $385.80$| $461.35$| $480.67 + 2(461.35) = \mathbf{1403.37}$| **$300$ RPS** | 🛑 **DDoS 1200 RPS: Khóa cứng trần 300 RPS** |
| **9** | **$70.0$** | $0.3(70) + 0.7(480.67) = \mathbf{357.47}$| $[180, 240, 250, 1200, 70]$| $388.00$| $460.67$| $357.47 + 2(460.67) = \mathbf{1278.81}$| **$300$ RPS** | Hết tấn công, tải hạ |

### Chi tiết phép tính tại Chu kỳ 5 (Bắt đầu có Spike):
1. Mẫu RPS ghi nhận: $X(5) = 180.0\text{ RPS}$.
2. Cập nhật EWMA:
   $$\text{EWMA}(5) = 0.3 \times 180.0 + 0.7 \times 59.82 = 54.0 + 41.87 = 95.87\text{ RPS}$$
3. Mảng lịch sử 5 mẫu: $\{62, 58, 61, 59, 180\}$.
   - Trung bình mẫu: $\overline{X}_5 = \frac{62 + 58 + 61 + 59 + 180}{5} = \frac{420}{5} = 84.0\text{ RPS}$.
   - Tổng bình phương sai lệch:
     $$(62-84)^2 + (58-84)^2 + (61-84)^2 + (59-84)^2 + (180-84)^2$$
     $$= (-22)^2 + (-26)^2 + (-23)^2 + (-25)^2 + (96)^2 = 484 + 676 + 529 + 625 + 9216 = 11530$$
   - Phương sai mẫu hiệu chỉnh: $s^2 = \frac{11530}{5 - 1} = \frac{11530}{4} = 2882.5$.
   - Độ lệch chuẩn mẫu: $s = \sqrt{2882.5} \approx 53.74\text{ RPS}$.
4. Tính toán ngưỡng thô:
   $$\text{Threshold}_{\text{raw}} = 95.87 + 2.0 \times 53.74 = 95.87 + 107.48 = 203.35\text{ RPS}$$
5. Kẹp biên an toàn:
   $$\text{Threshold} = \min(300, \max(50, 203)) = 203\text{ RPS}$$
$\implies$ **Kết quả:** Ngưỡng ngay lập tức được nới rộng từ $63 \text{ RPS} \to 203 \text{ RPS}$, giúp toàn bộ đợt tăng tải hợp lệ $180 \text{ RPS}$ được thông qua thuận lợi mà không bị chặn nhầm!

---

# PHẦN IV: CÁC THUẬT TOÁN THÍCH ỨNG LIÊN QUAN (RELATED WORKS)

Để làm rõ đóng góp của đề tài, phần này phân tích chi tiết toán học của 2 giải pháp thích ứng kinh điển trong công nghiệp:

## 1. Netflix Concurrency Limits (TCP Vegas Gradient Algorithm)

### 1.1. Bối cảnh và nguyên lý
Được Netflix phát triển dựa trên giải thuật kiểm soát tắc nghẽn mạng **TCP Vegas**. Thuật toán không đo RPS mà đo **thời gian đáp ứng (RTT - Round Trip Time)** của dịch vụ. Khi hệ thống bắt đầu quá tải, hàng đợi nội bộ bị ứ đọng làm RTT tăng vọt. Thuật toán sẽ tính toán hệ số suy giảm Gradient để tự động hạ giới hạn số lượng request xử lý đồng thời (Concurrency Limit).

### 1.2. Công thức toán học
1. **Tính hệ số Gradient ($G$):**
   $$\text{Gradient} = \frac{RTT_{\text{no\_load}}}{RTT_{\text{actual}}}$$
   - $RTT_{\text{no\_load}}$: RTT cơ sở tối thiểu đo được khi hệ thống hoàn toàn rảnh rỗi.
   - $RTT_{\text{actual}}$: RTT trung bình trượt đo được trong chu kỳ gần nhất.
   - Nếu $RTT_{\text{actual}} \approx RTT_{\text{no\_load}} \implies \text{Gradient} \approx 1.0$ (Hệ thống khỏe mạnh).
   - Nếu $RTT_{\text{actual}} > RTT_{\text{no\_load}} \implies \text{Gradient} < 1.0$ (Hệ thống đang bị nghẽn).

2. **Cập nhật Concurrency Limit mới:**
   $$\text{Limit}_{\text{new}} = \text{Limit}_{\text{old}} \times \text{Gradient} + \beta$$
   *(với $\beta$ là hằng số nới lỏng dự phòng, thường chọn $\beta \in [1, 3]$).*

### 1.3. Ví dụ số học
- Giả sử $RTT_{\text{no\_load}} = 20\text{ ms}$, $\text{Limit}_{\text{old}} = 100$ concurrent requests, $\beta = 2$.
- Khi Backend gặp sự cố chậm DB, $RTT_{\text{actual}}$ tăng vọt lên $80\text{ ms}$.
- Tính Gradient: $\text{Gradient} = \frac{20}{80} = 0.25$.
- Cập nhật Limit mới: $\text{Limit}_{\text{new}} = 100 \times 0.25 + 2 = 25 + 2 = 27$ concurrent requests.
- **Hạn chế so với đề tài:** Phải nhúng thư viện SDK vào sâu trong từng Microservice (xâm lấn mã nguồn), không thực hiện được tại Ingress Gateway phía ngoài.

---

## 2. Google SRE Adaptive Throttling (Client-side Drop Probability)

### 2.1. Bối cảnh và nguyên lý
Được Google công bố trong cuốn sách *Site Reliability Engineering (SRE)*. Thuật toán chạy **tại Client (hoặc API Consumer SDK)**. Client tự theo dõi tỷ lệ giữa tổng số request mình đã gửi đi và số request được Backend chấp thuận xử lý thành công. Khi Backend bắt đầu trả về lỗi quá tải ($HTTP 429$ hoặc $HTTP 503$), Client sẽ chủ động tự hủy (Drop) một tỷ lệ request nhất định ngay tại máy khách trước khi gửi qua đường truyền mạng.

### 2.2. Công thức toán học xác suất loại bỏ
$$P_{\text{drop}} = \max\left(0, \; \frac{\text{requests} - K \times \text{accepts}}{\text{requests} + 1}\right)$$

*Giải thích đại lượng:*
- $\text{requests}$: Tổng số request mà tầng ứng dụng Client muốn phát đi trong cửa sổ thời gian gần nhất (ví dụ $1$ đến $2$ phút).
- $\text{accepts}$: Số lượng request được Backend phản hồi thành công (không bị lỗi quá tải).
- $K$: Hệ số nhân độ chịu tải (*Multiplier / Aggressiveness Factor*), thông thường Google khuyến nghị chọn $K = 2.0$ (hoặc $K = 1.1$ đối với hệ thống nghiêm ngặt).
  - Khi $K = 2.0$, Backend chấp thuận 1 request thì Client được phép gửi tối đa 2 requests trước khi thuật toán bắt đầu loại bỏ.

### 2.3. Ví dụ số học
Giả sử chọn hệ số $K = 2.0$.
1. **Trường hợp Backend khỏe mạnh:**
   - Client gửi $100$ requests, Backend chấp nhận toàn bộ $100$ requests ($\text{requests} = 100, \text{accepts} = 100$).
   - Tính tử số: $\text{requests} - K \times \text{accepts} = 100 - (2.0 \times 100) = -100$.
   - $P_{\text{drop}} = \max\left(0, \frac{-100}{101}\right) = 0 \implies \mathbf{Không \; drop \; request \; nào}$.
2. **Trường hợp Backend bị quá tải nghẽn mạng:**
   - Client muốn gửi $1000$ requests, nhưng Backend chỉ đủ sức tiếp nhận $200$ requests (còn lại $800$ requests bị lỗi quá tải).
   - $\text{requests} = 1000, \text{accepts} = 200$.
   - Tính tử số: $1000 - (2.0 \times 200) = 1000 - 400 = 600$.
   - $P_{\text{drop}} = \max\left(0, \frac{600}{1000 + 1}\right) = \frac{600}{1001} \approx \mathbf{0.5994 \; (59.94\%)}$.
   - Client sẽ tung xúc xắc ngẫu nhiên: Với mỗi request chuẩn bị gửi đi, có xác suất xấp xỉ **$60\%$ request bị hủy ngay tại Client**, chỉ có $40\%$ request thực sự được phát qua mạng, giúp giảm tải tức thời $60\%$ cho Backend đang hấp hối.
- **Hạn chế so với đề tài:** Phụ thuộc hoàn toàn vào sự hợp tác tự giác của Client. Trong thực tế, các cuộc tấn công DDoS hoặc Client độc hại sẽ cố tình bỏ qua cơ chế này để tiếp tục bắn phá hệ thống. Do đó, việc bảo vệ tại biên Ingress Gateway như đề tài là bắt buộc.

---

# PHẦN V: CÁC ĐẠI LƯỢNG ĐO LƯỜNG VÀ ĐÁNH GIÁ THỰC NGHIỆM

Để đánh giá khoa học và định lượng chính xác hiệu năng của các giải pháp, toàn bộ thực nghiệm sử dụng hệ thống chỉ số đo lường chuẩn hóa sau:

```
                                  TỔNG SỐ REQUEST GỬI ĐẾN (N_total)
                                                |
                       +------------------------+------------------------+
                       |                                                 |
                       v                                                 v
           ĐƯỢC GATEWAY CHẤP THUẬN (HTTP 200)               BỊ GATEWAY TỪ CHỐI (HTTP 429)
                       |                                                 |
                       v                                                 v
           Thông lượng hiệu dụng:                             Tỷ lệ chặn tổng thể:
           RPS_success = N_200 / delta_T                      P_reject = (N_429 / N_total) * 100%
                                                                         |
                                              +--------------------------+--------------------------+
                                              |                                                     |
                                              v                                                     v
                                     CHẶN ĐÚNG LƯU LƯỢNG DDoS                         CHẶN NHẦM KHÁCH HÀNG HỢP LỆ
                                     (Bảo vệ thành công Backend)                      Tỷ lệ chặn nhầm (False Positive):
                                                                                      P_FP = (N_429_legit / N_total_legit) * 100%
```

### 1. Thông lượng hiệu dụng thành công (Effective Throughput - $RPS_{\text{success}}$)
Phản ánh năng lực phục vụ thực tế của hệ thống đối với các request hợp lệ:
$$RPS_{\text{success}} = \frac{N_{200}}{\Delta T}$$
- $N_{200}$: Tổng số lượng request nhận mã phản hồi HTTP 200 OK thành công.
- $\Delta T$: Khoảng thời gian đo lường thực nghiệm tính bằng giây.

### 2. Tỷ lệ chặn tổng thể (Overall Reject Rate - $P_{\text{reject}}$)
Tỷ lệ phần trăm các request bị Gateway chặn lại bằng mã phản hồi HTTP 429:
$$P_{\text{reject}} = \frac{N_{429}}{N_{\text{total}}} \times 100\% = \frac{N_{429}}{N_{200} + N_{429} + N_{5xx}} \times 100\%$$

### 3. Tỷ lệ chặn nhầm request hợp lệ (False Positive Rate - $P_{\text{FP}}$)
Chỉ số quan trọng nhất để đánh giá trải nghiệm người dùng trong kịch bản bùng nổ lưu lượng (Flash Sales / Spike Load):
$$P_{\text{FP}} = \frac{N_{429, \text{legitimate}}}{N_{\text{total}, \text{legitimate}}} \times 100\%$$
- Một cơ chế Rate Limiting tĩnh xuất sắc về mặt bảo mật thường có $P_{\text{FP}}$ rất cao ($> 60\%$), gây thiệt hại doanh thu nghiêm trọng.
- Mục tiêu của giải pháp Adaptive Threshold đề xuất là giảm $P_{\text{FP}}$ xuống **$< 5\%$**.

### 4. Phân vị thời gian phản hồi (Percentile Latencies: $p_{50}, p_{95}, p_{99}$)
Cho biết ngưỡng thời gian phản hồi mà $X\%$ số request được xử lý nhanh hơn hoặc bằng giá trị đó.
- **Định nghĩa toán học:** Cho tập hợp thời gian phản hồi đã sắp xếp tăng dần $\mathcal{L} = \{l_1, l_2, \dots, l_N\}$ với $l_1 \le l_2 \le \dots \le l_N$:
  $$\text{Index}_p = \lceil \frac{p}{100} \times N \rceil$$
  $$p_X = l_{\text{Index}_X}$$
- **$p_{50}$ (Median):** Thời gian phản hồi điển hình của đa số người dùng thông thường.
- **$p_{95}, p_{99}$ (Tail Latency):** Thời gian phản hồi của nhóm người dùng gặp độ trễ lớn nhất (bị ảnh hưởng bởi nghẽn Thread, GC Pause, Network Jitter). Một hệ thống tốt phải giữ cho $p_{95}, p_{99}$ luôn ổn định dưới tải cao.

---

# TỔNG KẾT MỐI QUAN HỆ TOÀN DIỆN GIỮA CÁC THUẬT TOÁN

| Tầng kiến trúc | Thành phần đảm nhiệm | Thuật toán cốt lõi | Đại lượng đầu vào | Đại lượng đầu ra | Độ phức tạp |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Data Plane** *(Edge Gateway)* | Kong Gateway + Redis Lua Script | **Sliding Window Counter** | $\text{Count}_{\text{prev}}, \text{Count}_{\text{curr}}, \text{weight}, \text{Threshold}(t)$ | Quyết định ALLOW (1) / DENY (0) | Thời gian: $O(1)$<br>Không gian: $O(1)$ |
| **Control Plane** *(Controller)* | Adaptive Python Engine | **EWMA + Rolling Sample StdDev** | Chuỗi chu kỳ RPS $X(t)$ từ Prometheus | Ngưỡng động thích ứng $\text{Threshold}(t)$ | Thời gian: $O(W)$<br>Không gian: $O(W)$ |
| **Downstream** *(Tài nguyên)* | Spring Boot Microservice | Business Logic | Requests được phép đi qua Ingress | HTTP 200 Response + Metrics | Năng lực chịu tải: $\approx 300\text{ RPS}$ |
