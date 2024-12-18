import psycopg2
import os
import sys

from dotenv import load_dotenv


def main(connection, email):
    prefix = "t4dbasic_"  # all old emails are prefixed with this

    cursor = connection.cursor()
    cursor.execute("SELECT * FROM ab_user WHERE email LIKE %s;", (f"%{email}%",))
    rows = cursor.fetchall()

    column_names = [desc[0] for desc in cursor.description]

    rows_dict = [dict(zip(column_names, row)) for row in rows]
    # sort by created_on
    rows_dict = sorted(rows_dict, key=lambda x: x["created_on"])

    if len(rows_dict) == 0:
        print("No record found for email - ", email, "\n")
    elif len(rows_dict) == 2:
        print("2 records found for email - ", email, "\n")

        old_one = rows_dict[0]

        new_one = rows_dict[1]

        if old_one and new_one:
            print("Both old and new records found\n")
            print("Old record found - ", old_one["username"], "\n")
            print("New record found - ", new_one["username"], "\n")

            if not old_one["email"].startswith(prefix):
                print(
                    f"Exiting: The email does not have prefix {prefix}. Please check again. Looks like the script has already ran"
                )
                return True

            if old_one["username"].startswith("google"):
                print(
                    "Exiting: The record is already a Google OAuth record. Please check again"
                )
                return True

            cols_to_copy = [
                # "first_name",
                # "last_name",
                "username",
                "password",
                "email",
                # "last_login",
                # "login_count",
                # "fail_login_count",
                # "created_on",
                # "changed_on",
                # "blob",
            ]
            try:
                # start a transaction
                cursor.execute("BEGIN;")

                # since there is unique constraint on email & username
                # set them to some temp values for now
                # we will revert everything if it fails

                update_old_query = f"""
                UPDATE ab_user
                SET email = %s, username = %s
                WHERE id = %s;
                """
                cursor.execute(
                    update_old_query,
                    (
                        f"{old_one['email']}_temp",
                        f"{old_one['username']}_temp",
                        old_one["id"],
                    ),
                )

                update_new_query = f"""
                UPDATE ab_user
                SET email = %s, username = %s
                WHERE id = %s;
                """
                cursor.execute(
                    update_new_query,
                    (
                        f"{new_one['email']}_temp",
                        f"{new_one['username']}_temp",
                        new_one["id"],
                    ),
                )

                # now copy from one to another
                set_clause = ", ".join([f"{col} = %s" for col in cols_to_copy])

                update_old_query = f"""
                UPDATE ab_user
                SET {set_clause}
                WHERE id = %s;
                """
                cursor.execute(
                    update_old_query,
                    (*[new_one[col] for col in cols_to_copy], old_one["id"]),
                )

                update_new_query = f"""
                UPDATE ab_user
                SET {set_clause}
                WHERE id = %s;
                """
                cursor.execute(
                    update_new_query,
                    (*[old_one[col] for col in cols_to_copy], new_one["id"]),
                )

                connection.commit()
                print("Records updated successfully.")

            except (Exception, psycopg2.Error) as error:
                print("Error while updating records", error)
                connection.rollback()
                return False

        return True

    return False


if __name__ == "__main__":
    load_dotenv()

    connection = psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        database=os.getenv("database"),
    )

    # fetch all old users that start with "t4basic_"
    cursor = connection.cursor()
    cursor.execute("SELECT email FROM ab_user where email like 't4dbasic_%';")
    rows = [row[0] for row in cursor.fetchall()]
    count_old = len(rows)
    print("Total old users found - ", count_old)

    # migrate to oauth if they have signed up with google
    users_migrate_to_oauth = 0
    for old_email in rows:
        new_email = old_email.replace("t4dbasic_", "")
        print("Old email - ", old_email, "New email - ", new_email)
        if main(connection, new_email):
            users_migrate_to_oauth += 1

    print("Total users migrated to OAuth - %d", users_migrate_to_oauth)
    connection.close()
