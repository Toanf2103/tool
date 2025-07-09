const webhookUrl = 'https://discord.com/api/webhooks/1384939190135029943/KWP2X_J8l8J3r2Uys-kEJZbzYDS4JfGdgXyCKZ_0aGLV1QWH5UNeXKWmOcoaK-pja5zN';

const data = {
  content: '✅ Gửi thành công từ JavaScript!',
  username: 'Bot JS'
};

fetch(webhookUrl, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify(data)
})
.then(response => {
  if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
  return response.text();
})
.then(result => {
  console.log('✅ Đã gửi:', result);
})
.catch(error => {
  console.error('❌ Lỗi khi gửi webhook:', error.message);
});
