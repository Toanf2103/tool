const nodemailer = require('nodemailer');

/**
 * Hàm gửi email với cấu hình MailSettings
 * @param {Object} mailSettings - Cấu hình email
 * @param {string} toEmail - Email người nhận
 * @param {string} subject - Tiêu đề email (optional)
 * @param {string} content - Nội dung email (optional)
 */
async function sendEmail(mailSettings, toEmail, subject = 'Test Email', content = '123') {
    try {
        // Tạo transporter với cấu hình từ MailSettings
        const transporter = nodemailer.createTransport({
            host: mailSettings.Host,
            port: mailSettings.Port,
            secure: false,
            auth: {
                user: mailSettings.Account,
                pass: mailSettings.Password
            },
            tls: {
                minVersion: 'TLSv1.3'
            }
        });

        // Cấu hình email
        const mailOptions = {
            from: `"${mailSettings.DisplayName}" <${mailSettings.From}>`,
            to: toEmail,
            subject: subject,
            text: content, // Nội dung dạng text
            html: `<p>${content}</p>` // Nội dung dạng HTML
        };

        // Gửi email
        const info = await transporter.sendMail(mailOptions);
        
        console.log('Email đã được gửi thành công!');
        console.log('Message ID:', info.messageId);
        
        return {
            success: true,
            messageId: info.messageId,
            message: 'Email sent successfully'
        };

    } catch (error) {
        console.error('Lỗi khi gửi email:', error);
        
        return {
            success: false,
            error: error.message,
            message: 'Failed to send email'
        };
    }
}

// Ví dụ sử dụng
const mailSettings = {
    "Host": "mail.kas.asia",
    "Port": 587,
    "From": "posone@kas.asia",
    "Password": "Kas!@#123",
    "Account": "posone@kas.asia",
    "DisplayName": "KAS POS"
};

// Gọi hàm gửi email
async function testSendEmail() {
    const result = await sendEmail(
        mailSettings, 
        'nobitakute002111@gmail.com', // Email người nhận
        'Test Subject',          // Tiêu đề
        '123'                    // Nội dung
    );
    
    console.log('Kết quả:', result);
}

// Uncomment dòng dưới để test
testSendEmail();