import os
import requests


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def main():
    '''Creates tree diagrams of the categories'''
    # download json
    url = "https://shopee.co.id/api/v1/category_list/"
    r = requests.get(url)
    json_obj = r.json()

    # create text files to store lists of category ids
    main_file = open(os.path.join(ROOT, "data", "main.txt"), "w")
    sub1_file = open(os.path.join(ROOT, "data", "sub1.txt"), "w")
    sub2_file = open(os.path.join(ROOT, "data", "sub2.txt"), "w")

    # iterate over categories
    for cat in json_obj:
        # record category id
        main_json = cat['main']
        main_file.write('{}\n'.format(main_json['catid']))

        # create and write to tree diagram file
        main_name = main_json['name']
        cat_tree_file = open(os.path.join(ROOT, 'trees', '{}.txt'.format(main_name)), "w")
        cat_tree_file.write('{}\n'.format(main_name))
        cat_tree_file.write('{:>5}\n'.format("|"))

        # iterate over sub1 categories
        for sub1_json in cat['sub']:
            # record category id
            sub1_file.write('{}\n'.format(sub1_json['catid']))

            # write to tree diagram file
            sub1_name = sub1_json['name']
            cat_tree_file.write('{:>5}'.format("|"))
            cat_tree_file.write(' - {}\n'.format(sub1_name))
            cat_tree_file.write('{:>5}{:>5}\n'.format("|", "|"))

            # iterate over sub2 categories
            for sub2_json in sub1_json['sub_sub']:
                # record category id
                sub2_file.write('{}\n'.format(sub2_json['catid']))

                # write to tree diagram
                sub2_name = sub2_json['name']
                cat_tree_file.write('{:>5}{:>5}'.format("|", "|"))
                cat_tree_file.write(' - {}\n'.format(sub2_name))

            cat_tree_file.write('{:>5}\n'.format("|"))

        # close tree diagram file
        cat_tree_file.close()

    # close all my cateogry files
    main_file.close()
    sub1_file.close()
    sub2_file.close()


if __name__ == '__main__':
    main()
