if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY || !process.env.AWS_ACCOUNT_ID) {
  throw new Error(
    'AWS Credentials or AWS Account Id not found in Environment Variables.' + '\n- AWS_ACCESS_KEY_ID\n- AWS_SECRET_ACCESS_KEY\n- AWS_ACCOUNT_ID\n'
  );
}
